package bmc

import (
	"context"
	"errors"
	"fmt"

	"github.com/gebn/bmc/pkg/ipmi"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/gopacket"
)

var (
	errSDRRepositoryModified = errors.New(
		"the SDR Repository was modified during enumeration")
)

// SDRRepository is a retrieved SDR Repository. For the time being, this is a
// collection of Full Sensor Records, indexed by record ID. Note that because
// this is a map, iteration order is randomised and almost definitely not the
// same as retrieval order, which has no guarantees anyway.
type SDRRepository map[ipmi.RecordID]*ipmi.FullSensorRecord

// RetrieveSDRRepository enumerates all Full Sensor Records in the BMC's SDR
// Repository. This method will back-off if an error occurs, or it detects a
// change mid-way through iteration, which would invalidate records retrieved so
// far. The session-configured timeout is used for individual commands.
func RetrieveSDRRepository(ctx context.Context, s Session) (SDRRepository, error) {
	var repo *SDRRepository
	err := backoff.Retry(func() error {
		initialInfo, err := s.GetSDRRepositoryInfo(ctx)
		if err != nil {
			return err
		}
		// we could error here if unsupported SDR Repo version; no such cases
		// currently exist
		candidateRepo, err := walkSDRs(ctx, s)
		if err != nil {
			return err
		}
		finalInfo, err := s.GetSDRRepositoryInfo(ctx)
		if err != nil {
			return err
		}
		if initialInfo.LastAddition.Before(finalInfo.LastAddition) ||
			initialInfo.LastErase.Before(finalInfo.LastErase) {
			// tough luck, start again
			return errSDRRepositoryModified
		}
		repo = &candidateRepo
		return nil
	}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
	if err != nil {
		return nil, err
	}
	return *repo, nil
}

// walkSDRs iterates over the SDR Repository. It is not concerned with the repo
// changing behind its back.
func walkSDRs(ctx context.Context, s Session) (SDRRepository, error) {
	repo := SDRRepository{} // we could set a size; it's a micro-optimisation

	reserveSDRRepoCmd := &ipmi.ReserveSDRRepositoryCmd{}
	if err := ValidateResponse(s.SendCommand(ctx, reserveSDRRepoCmd)); err != nil {
		return nil, err
	}
	getSDRCmd := &ipmi.GetSDRCmd{
		Req: ipmi.GetSDRReq{
			RecordID:      ipmi.RecordIDFirst,
			Length:        0x05,                                // only request the header
			ReservationID: reserveSDRRepoCmd.Rsp.ReservationID, // needed for partial readings
		},
	}

	// it's ambiguous whether we retrieve ipmi.RecordIDLast; other
	// implementations do not. The final SDR seems to have two RecordIDs - a
	// "normal" one and ipmi.RecordIDLast, so retrieving ipmi.RecordIDLast will
	// duplicate it.
	for getSDRCmd.Req.RecordID != ipmi.RecordIDLast {
		if err := ValidateResponse(s.SendCommand(ctx, getSDRCmd)); err != nil {
			return nil, err
		}

		headerPacket := gopacket.NewPacket(getSDRCmd.Rsp.Payload, ipmi.LayerTypeSDR,
			gopacket.DecodeOptions{
				Lazy: true,
				// we can't set NoCopy because we reuse getSDRCmd.Rsp
			})
		if headerPacket == nil || len(headerPacket.Layers()) == 0 {
			return nil, fmt.Errorf("invalid SDR: missing SDR layer: %v", getSDRCmd)
		}
		sdr := headerPacket.Layers()[0].(*ipmi.SDR)
		if sdr.Type == ipmi.RecordTypeFullSensor {
			if sdr.Length > 59 {
				// SDR exceeds the specified length of 64. Need to read in chunks.
				return nil, fmt.Errorf("invalid SDR: length exceeds 64 bytes: %v", sdr.Length)
			}

			getSDRCmd.Req.Offset = 0x05
			getSDRCmd.Req.Length = sdr.Length
			if err := ValidateResponse(s.SendCommand(ctx, getSDRCmd)); err != nil {
				return nil, err
			}

			fsrPacket := gopacket.NewPacket(getSDRCmd.Rsp.Payload, ipmi.LayerTypeFullSensorRecord,
				gopacket.DecodeOptions{
					Lazy: true,
				})
			if fsrPacket == nil || len(fsrPacket.Layers()) == 0 {
				return nil, fmt.Errorf("invalid SDR: missing FSR layer: %v", getSDRCmd)
			}
			fsr := fsrPacket.Layers()[0].(*ipmi.FullSensorRecord)
			repo[getSDRCmd.Req.RecordID] = fsr
		}

		getSDRCmd.Req.RecordID = getSDRCmd.Rsp.Next
		getSDRCmd.Req.Offset = 0x00
		getSDRCmd.Req.Length = 0x05
	}
	return repo, nil
}
