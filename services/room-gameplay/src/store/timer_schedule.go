package store

import (
	"context"

	"unoarena/services/room-gameplay/domain"
)

func scheduleTimersFromSession(ctx context.Context, timers *TimerIndex, sess *domain.Session) error {
	if timers == nil || sess == nil || sess.Room() == nil {
		return nil
	}
	room := sess.Room()
	roomID := string(room.ID())
	for pid, ds := range room.DisconnectsMap() {
		if !ds.Active {
			continue
		}
		id := TimerID{
			Family: timerFamilyReconnect, RoomID: roomID, PlayerID: string(pid),
			Version: int64(ds.DisconnectVersion), ExpiresAt: ds.DeadlineUTC.UTC(),
		}
		if err := timers.Schedule(ctx, id); err != nil {
			return err
		}
	}
	if uw, ok := room.UnoWindow(); ok && uw.IsOpen() {
		id := TimerID{
			Family: timerFamilyUno, RoomID: roomID, PlayerID: string(uw.PlayerID),
			GameID: string(uw.GameID), Trigger: string(uw.TriggeringGameEventID),
			OpeningSeq: int64(uw.OpeningSequence), ExpiresAt: uw.ExpiresAt.UTC(),
		}
		if err := timers.Schedule(ctx, id); err != nil {
			return err
		}
	}
	if g := sess.Game(); g != nil {
		if gu := g.UnoWindow(); gu != nil && gu.IsOpen() {
			id := TimerID{
				Family: timerFamilyUno, RoomID: roomID, PlayerID: string(gu.PlayerID),
				GameID: string(g.ID()), Trigger: "engine-uno",
				OpeningSeq: int64(gu.OpeningSequence), ExpiresAt: gu.ExpiresAt.UTC(),
			}
			if err := timers.Schedule(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}
