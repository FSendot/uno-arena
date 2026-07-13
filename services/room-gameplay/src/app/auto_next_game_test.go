package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/envelope"
)

func TestAutoNextGame_StartsGameTwoAfterAcceptedGameCompleted(t *testing.T) {
	env, sess := newAutoNextEnv(t)
	storeSession(t, env.sessions, sess)
	completedSeq := int64(sess.Room().Sequence())

	result := playWinningCard(env.svc, "win-game-1", completedSeq)
	if result.Err != nil || result.Result.Status != envelope.StatusAccepted {
		t.Fatalf("originating play=%+v", result)
	}

	live, _ := env.sessions.Get(context.Background(), domain.RoomID("room_auto"))
	if live.Room().GameCompletedInMatch() {
		t.Fatal("automatic StartNextGame must clear completed-game marker")
	}
	if live.Game() == nil || live.Game().Completed() {
		t.Fatal("game two must be active")
	}
	if got, want := string(live.GameID()), "game-next-game-1"; got != want {
		t.Fatalf("game id=%q want %q", got, want)
	}
	if env.deals.DealCalls != 1 || env.deals.ConfirmCalls != 1 {
		t.Fatalf("deal calls=%d confirms=%d want 1/1", env.deals.DealCalls, env.deals.ConfirmCalls)
	}
	if env.integrity.Len() != 2 {
		t.Fatalf("GI appends=%d want PlayCard + StartNextGame", env.integrity.Len())
	}
}

func TestAutoNextGame_TerminalBestOfThreeDoesNotStartGameThree(t *testing.T) {
	env, sess := newAutoNextEnv(t)

	first := sess.PlayCard(domain.PlayCardCommand{
		CommandID: "direct-win-1", PlayerID: "host", CardID: "host-win-1",
		ExpectedSequence: sess.Room().Sequence(), NowUTC: time.Now().UTC(),
	})
	if first.Rejection != nil || !domain.HasFact(first.Facts, domain.FactGameCompleted) {
		t.Fatalf("first completion=%+v", first)
	}
	secondDeal := winningDeal("host-win-2")
	second := sess.StartNextGame(domain.StartNextGameCommand{
		CommandID: "direct-next", GameID: "game-2", ExpectedSequence: sess.Room().Sequence(), Deal: secondDeal,
	})
	if second.Rejection != nil {
		t.Fatalf("second start=%+v", second)
	}
	storeSession(t, env.sessions, sess)

	result := playCard(env.svc, "win-game-2", "host-win-2", int64(sess.Room().Sequence()))
	if result.Err != nil || result.Result.Status != envelope.StatusAccepted {
		t.Fatalf("terminal play=%+v", result)
	}
	live, _ := env.sessions.Get(context.Background(), domain.RoomID("room_auto"))
	if live.Room().Status() != domain.RoomStatusCompleted || live.Match() == nil || !live.Match().Completed() {
		t.Fatalf("best-of-three must be terminal: status=%s match=%+v", live.Room().Status(), live.Match())
	}
	if got := env.deals.DealCalls; got != 0 {
		t.Fatalf("terminal match reserved %d next deals", got)
	}
	if got := env.integrity.Len(); got != 1 {
		t.Fatalf("GI appends=%d want terminal PlayCard only", got)
	}
}

func TestAutoNextGame_DuplicateOriginatingCommandDoesNotStartTwice(t *testing.T) {
	env, sess := newAutoNextEnv(t)
	storeSession(t, env.sessions, sess)
	seq := int64(sess.Room().Sequence())

	first := playWinningCard(env.svc, "same-win", seq)
	second := playWinningCard(env.svc, "same-win", seq)
	if first.Err != nil || second.Err != nil || second.Result.Status != envelope.StatusAccepted {
		t.Fatalf("first=%+v duplicate=%+v", first, second)
	}
	if env.deals.DealCalls != 1 || env.integrity.Len() != 2 {
		t.Fatalf("duplicate caused side effects: deals=%d GI=%d", env.deals.DealCalls, env.integrity.Len())
	}
}

func TestAutoNextGame_FailurePreservesCompletionAndDuplicateRetriesSameFollowUp(t *testing.T) {
	env, sess := newAutoNextEnv(t)
	storeSession(t, env.sessions, sess)
	seq := int64(sess.Room().Sequence())
	env.deals.DealFn = func(_, _ string, _ []string) (game.DealMaterial, error) {
		return game.DealMaterial{}, errors.New("GI deal unavailable")
	}

	first := playWinningCard(env.svc, "retry-win", seq)
	if first.Err != nil || first.Result.Status != envelope.StatusAccepted {
		t.Fatalf("committed originating play must remain accepted: %+v", first)
	}
	live, _ := env.sessions.Get(context.Background(), domain.RoomID("room_auto"))
	if !live.Room().GameCompletedInMatch() || live.Game() == nil || !live.Game().Completed() {
		t.Fatal("failed follow-up must leave durable completed-game marker for retry")
	}
	if env.integrity.Len() != 1 {
		t.Fatalf("failed reservation must not append StartNextGame to GI: %d", env.integrity.Len())
	}

	// Retrying the already accepted originating command re-enters the policy.
	env.deals.DealFn = nil
	retry := playWinningCard(env.svc, "retry-win", seq)
	if retry.Err != nil || retry.Result.Status != envelope.StatusAccepted {
		t.Fatalf("retry=%+v", retry)
	}
	live, _ = env.sessions.Get(context.Background(), domain.RoomID("room_auto"))
	if live.Room().GameCompletedInMatch() || live.Game() == nil || live.Game().Completed() {
		t.Fatal("retry must start exactly one active next game")
	}
	if env.deals.DealCalls != 2 || env.integrity.Len() != 2 {
		t.Fatalf("retry effects: deals=%d GI=%d want 2 attempts/2 committed commands", env.deals.DealCalls, env.integrity.Len())
	}
}

func newAutoNextEnv(t *testing.T) (*reservationEnv, *domain.Session) {
	t.Helper()
	env := newReservationEnv(t)
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "create", RoomID: "room_auto", HostID: "host",
		Visibility: domain.VisibilityPublic, MaxSeats: 4,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	join := room.JoinRoom(domain.JoinRoomCommand{CommandID: "join", PlayerID: "guest", ExpectedSequence: room.Sequence()})
	if join.Rejection != nil {
		t.Fatal(join.Rejection)
	}
	lock := room.LockRoom(domain.LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: room.Sequence()})
	if lock.Rejection != nil {
		t.Fatal(lock.Rejection)
	}
	sess := domain.OpenSession(room)
	start := sess.StartMatch(domain.StartMatchCommand{
		CommandID: "start", ActorID: "host", GameID: "game-1", ExpectedSequence: room.Sequence(),
	}, winningDeal("host-win-1"))
	if start.Rejection != nil {
		t.Fatal(start.Rejection)
	}
	return env, sess
}

func winningDeal(hostCard game.CardID) game.DealMaterial {
	return game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"host":  {{ID: hostCard, Color: game.ColorRed, Face: game.Face3}},
			"guest": {{ID: "guest-card-" + hostCard, Color: game.ColorBlue, Face: game.Face2}},
		},
		DiscardTop:      game.Card{ID: "discard-" + hostCard, Color: game.ColorRed, Face: game.Face5},
		ActiveColor:     game.ColorRed,
		CurrentSeat:     0,
		Direction:       game.DirectionClockwise,
		DrawPileSize:    93,
		HasDrawPileSize: true,
	}
}

func storeSession(t *testing.T, repo *MemorySessionRepository, sess *domain.Session) {
	t.Helper()
	if err := repo.Commit(context.Background(), CommitRequest{Session: sess}); err != nil {
		t.Fatal(err)
	}
}

func playWinningCard(svc *Service, commandID string, seq int64) CommandResult {
	return playCard(svc, commandID, "host-win-1", seq)
}

func playCard(svc *Service, commandID, cardID string, seq int64) CommandResult {
	return svc.HandleCommand(context.Background(), CommandInput{
		CommandID: commandID, Type: CmdPlayCard, SchemaVersion: envelope.CurrentSchemaVersion,
		RoomID: "room_auto", PlayerID: "host", SessionID: "host-session",
		ExpectedSequenceNumber: &seq, Payload: MustJSON(map[string]string{"roomId": "room_auto", "cardId": cardID}),
	})
}
