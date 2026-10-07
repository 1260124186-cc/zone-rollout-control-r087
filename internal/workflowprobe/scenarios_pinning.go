package workflowprobe

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

// runActivatedRevisionPinning verifies that an activated plan stays pinned to
// the revision that was current at activation: appending after activation is
// rejected, later revisions already present in a snapshot remain historical
// content only, detail reads and restart recovery keep returning the
// activation-time revision, and observations recorded against the original
// serial still pass with the original thresholds.
func runActivatedRevisionPinning() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-pin-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-pin-02", "east"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "pin.example", 7001, 2, 1, 90)
	if err != nil {
		return err
	}

	// Emulate a snapshot written while the bug was live: a revision with a
	// higher serial was appended after activation and the plan summary was
	// moved to it.
	lateRevision := domain.ZoneRevision{
		ID:        "rev-late-7002",
		PlanID:    plan.ID,
		Serial:    7002,
		Authority: "ns1.pin.example",
		Entries: []domain.ZoneEntry{
			{Owner: "pin.example", Type: domain.EntryA, Value: "192.0.2.77"},
		},
		EntryCount: 1,
		CreatedAt:  harness.clock.Now().UTC().Add(time.Hour),
	}
	if err := harness.restart(func(state *domain.State) error {
		state.Revisions[lateRevision.ID] = lateRevision
		current := state.Plans[plan.ID]
		current.LatestRevisionID = lateRevision.ID
		state.Plans[plan.ID] = current
		return nil
	}); err != nil {
		return err
	}

	// After the restart, detail reads stay pinned to the activated revision.
	var reloaded domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &reloaded); err != nil {
		return err
	}
	if reloaded.LatestRevisionID != revision.ID || reloaded.ActivatedRevisionID != revision.ID {
		return ensure(false, "active plan does not stay pinned to the activated revision")
	}

	// Appending a newer revision after activation is rejected.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    7003,
		Authority: "ns1.pin.example",
		Entries: []domain.ZoneEntry{
			{Owner: "pin.example", Type: domain.EntryA, Value: "192.0.2.88"},
		},
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Observations recorded against the activated serial pass with the
	// original thresholds.
	group, err := allocate(harness, plan.ID, "pin-lease-0001", "east", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		if _, err := submitProbe(harness, leaseID, "pin-probe-"+leaseID, true, revision.Serial, 20+index); err != nil {
			return err
		}
	}
	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "pin-request-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.Verdict != domain.VerdictConsistent || assessment.RevisionID != revision.ID {
		return ensure(false, "assessment did not pass against the activated revision")
	}

	// A second restart keeps the same interpretation of the plan.
	if err := harness.restart(nil); err != nil {
		return err
	}
	var again domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &again); err != nil {
		return err
	}
	if again.LatestRevisionID != revision.ID || again.ActivatedRevisionID != revision.ID {
		return ensure(false, "restart changed the pinned revision")
	}
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "pin-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != assessment.ID {
		return ensure(false, "assessment replay returned another result")
	}

	// The later revision remains stored as historical content only and never
	// drives the active plan again.
	payload, err := os.ReadFile(harness.snapshotPath())
	if err != nil {
		return err
	}
	var snapshot domain.State
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return err
	}
	if _, ok := snapshot.Revisions[lateRevision.ID]; !ok {
		return ensure(false, "historical revision was dropped from the snapshot")
	}
	if snapshot.Plans[plan.ID].LatestRevisionID == lateRevision.ID {
		return ensure(false, "historical revision still drives the active plan")
	}

	// Draft plans still accept revisions normally.
	var draft domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "draft.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    50,
		ExpectedNodes:       1,
	}, &draft); err != nil {
		return err
	}
	var appended domain.ZoneRevision
	if err := harness.call(http.MethodPatch, "/v1/plans/"+draft.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    9001,
		Authority: "ns1.draft.example",
		Entries: []domain.ZoneEntry{
			{Owner: "draft.example", Type: domain.EntryA, Value: "192.0.2.99"},
		},
	}, &appended); err != nil {
		return err
	}
	if appended.Serial != 9001 {
		return ensure(false, "draft plan could not append a revision")
	}
	return nil
}
