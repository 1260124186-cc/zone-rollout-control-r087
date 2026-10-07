package store

import "solo-0014-zone-rollout-control/internal/domain"

const schemaVersion = 1

func blankState() domain.State {
	return normalize(domain.State{})
}

func normalize(state domain.State) domain.State {
	if state.SchemaVersion == 0 {
		state.SchemaVersion = schemaVersion
	}
	if state.Plans == nil {
		state.Plans = make(map[string]domain.RolloutPlan)
	}
	if state.Revisions == nil {
		state.Revisions = make(map[string]domain.ZoneRevision)
	}
	if state.Nodes == nil {
		state.Nodes = make(map[string]domain.ResolverNode)
	}
	if state.Leases == nil {
		state.Leases = make(map[string]domain.NodeLease)
	}
	if state.LeaseGroups == nil {
		state.LeaseGroups = make(map[string]domain.LeaseGroup)
	}
	if state.Probes == nil {
		state.Probes = make(map[string]domain.Probe)
	}
	if state.Assessments == nil {
		state.Assessments = make(map[string]domain.RolloutAssessment)
	}
	if state.LeaseRequestKeys == nil {
		state.LeaseRequestKeys = make(map[string]string)
	}
	if state.AssessmentRequestKeys == nil {
		state.AssessmentRequestKeys = make(map[string]string)
	}
	if state.AuditEvents == nil {
		state.AuditEvents = []domain.AuditEvent{}
	}
	reconcileRevisionBindings(&state)
	return state
}

func reconcileRevisionBindings(state *domain.State) {
	for planID, plan := range state.Plans {
		if plan.LatestRevisionID == "" && plan.ActivatedRevisionID == "" {
			continue
		}
		latestID := ""
		latestSerial := uint32(0)
		for revisionID, revision := range state.Revisions {
			if revision.PlanID != planID {
				continue
			}
			if latestID == "" || revision.Serial > latestSerial {
				latestID = revisionID
				latestSerial = revision.Serial
				continue
			}
			if revision.Serial == latestSerial && revisionID < latestID {
				latestID = revisionID
			}
		}
		if latestID == "" {
			continue
		}
		if plan.State == domain.PlanDraft {
			plan.LatestRevisionID = latestID
			state.Plans[planID] = plan
			continue
		}
		// Active and completed plans stay pinned to the revision that was
		// current at activation. Later revisions already stored in the
		// snapshot remain as historical content only.
		pinnedID := plan.ActivatedRevisionID
		if _, ok := state.Revisions[pinnedID]; pinnedID == "" || !ok {
			pinnedID = activatedFallbackID(plan, state.Revisions, latestID)
		}
		plan.ActivatedRevisionID = pinnedID
		plan.LatestRevisionID = pinnedID
		state.Plans[planID] = plan
	}
}

// activatedFallbackID derives the activation-time revision for snapshots that
// predate the activated_revision_id field: the highest-serial revision created
// no later than the activation timestamp. It falls back to the plan's latest
// revision when no such derivation is possible.
func activatedFallbackID(
	plan domain.RolloutPlan,
	revisions map[string]domain.ZoneRevision,
	latestID string,
) string {
	if plan.ActivatedAt == nil {
		return latestID
	}
	selectedID := ""
	selectedSerial := uint32(0)
	for revisionID, revision := range revisions {
		if revision.PlanID != plan.ID || revision.CreatedAt.After(*plan.ActivatedAt) {
			continue
		}
		if selectedID == "" || revision.Serial > selectedSerial {
			selectedID = revisionID
			selectedSerial = revision.Serial
			continue
		}
		if revision.Serial == selectedSerial && revisionID < selectedID {
			selectedID = revisionID
		}
	}
	if selectedID == "" {
		return latestID
	}
	return selectedID
}
