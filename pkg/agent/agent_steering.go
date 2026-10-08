// PicoClaw - Ultra-lightweight personal AI agent

package agent

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
)

func (al *AgentLoop) processMessageSync(ctx context.Context, msg bus.InboundMessage) {
	if al.channelManager != nil {
		defer al.channelManager.InvokeTypingStop(msg.Channel, msg.ChatID)
	}

	response, err := al.processMessage(ctx, msg)
	al.publishResponseOrError(ctx, msg.Channel, msg.ChatID, msg.SessionKey, response, err)
}

func (al *AgentLoop) runTurnWithSteering(ctx context.Context, initialMsg bus.InboundMessage, sessionKey string) {
	// The caller (Run) claimed the session before spawning this worker. Keep
	// that claim held for the whole turn plus the post-turn steering drain:
	// releasing it between the turn and the drain would let a new inbound
	// message claim the session for itself, which makes the drain's
	// continuation fail with "turn still active" and strands already queued
	// messages — they would then only be answered when the next turn starts,
	// injected before that message, so the agent answers the older question
	// first.
	claimReleased := false
	defer func() {
		if !claimReleased {
			al.releaseSessionTurnState(sessionKey, nil)
		}
	}()

	// Process the initial message, holding the session claim across the turn.
	response, err := al.processMessageWithClaim(ctx, initialMsg, true)
	if err != nil {
		if !al.maybePublishError(ctx, initialMsg.Channel, initialMsg.ChatID, initialMsg.SessionKey, err) {
			return // context canceled
		}
		response = ""
	}
	finalResponse := response

	// Build continuation target
	target, targetErr := al.buildContinuationTarget(initialMsg)
	if targetErr != nil {
		logger.WarnCF("agent", "Failed to build steering continuation target",
			map[string]any{
				"channel": initialMsg.Channel,
				"error":   targetErr.Error(),
			})
		return
	}
	if target == nil {
		// System message or non-routable, response already published
		return
	}

	for {
		continued, continueErr := al.drainQueuedSteeringContinuations(ctx, target, true)
		if continueErr != nil {
			logger.WarnCF("agent", "Failed to continue queued steering",
				map[string]any{
					"channel": target.Channel,
					"chat_id": target.ChatID,
					"error":   continueErr.Error(),
				})
		} else if continued != "" {
			finalResponse = continued
		}

		// Publish final response
		if finalResponse != "" {
			al.PublishResponseIfNeeded(ctx, target.Channel, target.ChatID, target.SessionKey, finalResponse)
			finalResponse = ""
		}

		if continueErr != nil {
			return
		}

		// Release the claim only when the queue is empty; if messages arrived
		// during the drain tail, keep the claim and drain them here.
		if al.releaseSessionClaimIfIdle(sessionKey) {
			claimReleased = true
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func (al *AgentLoop) drainQueuedSteeringContinuations(
	ctx context.Context,
	target *continuationTarget,
	claimHeld bool,
) (string, error) {
	if target == nil {
		return "", nil
	}

	finalResponse := ""
	for al.pendingSteeringCountForScope(target.SessionKey) > 0 {
		if err := ctx.Err(); err != nil {
			return finalResponse, err
		}

		logger.InfoCF("agent", "Continuing queued steering after turn end",
			map[string]any{
				"channel":     target.Channel,
				"chat_id":     target.ChatID,
				"session_key": target.SessionKey,
				"queue_depth": al.pendingSteeringCountForScope(target.SessionKey),
			})

		var continued string
		var continueErr error
		if claimHeld {
			continued, continueErr = al.runSteeringContinuation(ctx, target.SessionKey, target.Channel, target.ChatID, true)
		} else {
			continued, continueErr = al.Continue(ctx, target.SessionKey, target.Channel, target.ChatID)
		}
		if continueErr != nil {
			return finalResponse, continueErr
		}
		if continued == "" {
			break
		}
		finalResponse = continued
	}

	return finalResponse, nil
}

func (al *AgentLoop) resolveSteeringTarget(msg bus.InboundMessage) (string, string, bool) {
	if msg.Channel == "system" {
		return "", "", false
	}

	route, agent, err := al.resolveMessageRoute(msg)
	if err != nil || agent == nil {
		return "", "", false
	}
	allocation := al.allocateRouteSession(route, msg)

	return resolveScopeKey(allocation.SessionKey, msg.SessionKey), agent.ID, true
}
