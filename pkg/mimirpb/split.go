// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

// SplitWriteRequestByMaxMarshalSize splits the WriteRequest into multiple ones, where each partial WriteRequest marshalled size
// is at most maxSize. The input reqSize must be the value returned by WriteRequest.Size(); it's passed because the called
// may have already computed it.
//
// This function guarantees that a single Timeseries or Metadata entry is never split across multiple requests.
// For this reason, this function is a best-effort: if a single Timeseries or Metadata marshalled size is bigger
// than maxSize, then the returned partial WriteRequest marshalled size will be bigger than maxSize too.
//
// The returned partial WriteRequests are NOT a deep copy of the input one; they contain references to slices
// and data from the original WriteRequest.
//
// The returned requests may still retain references to fields in the original WriteRequest, i.e. they are tied to its lifecycle.
func SplitWriteRequestByMaxMarshalSize(req *WriteRequest, reqSize, maxSize int) []*WriteRequest {
	if reqSize <= maxSize {
		return []*WriteRequest{req}
	}

	partialReqsWithTimeseries := splitTimeseriesByMaxMarshalSize(req, reqSize, maxSize)
	partialReqsWithMetadata := splitMetadataByMaxMarshalSize(req, reqSize, maxSize)

	// Most of the time, a write request only have either Timeseries or Metadata.
	if len(partialReqsWithMetadata) == 0 {
		return partialReqsWithTimeseries
	}
	if len(partialReqsWithTimeseries) == 0 {
		return partialReqsWithMetadata
	}

	merged := make([]*WriteRequest, 0, len(partialReqsWithTimeseries)+len(partialReqsWithMetadata))
	merged = append(merged, partialReqsWithTimeseries...)
	merged = append(merged, partialReqsWithMetadata...)

	return merged
}

func splitTimeseriesByMaxMarshalSize(req *WriteRequest, reqSize, maxSize int) []*WriteRequest {
	if len(req.Timeseries) == 0 {
		return nil
	}

	newPartialReq := func() (*WriteRequest, int) {
		r := &WriteRequest{
			Source:              req.Source,
			SkipLabelValidation: req.SkipLabelValidation,
		}

		return r, r.Size()
	}

	// The partial requests returned by this function will not contain any Metadata,
	// so we first compute the request size without it.
	reqSizeWithoutMetadata := reqSize - req.MetadataSize()
	if reqSizeWithoutMetadata <= maxSize {
		partialReq, _ := newPartialReq()
		partialReq.Timeseries = req.Timeseries
		return []*WriteRequest{partialReq}
	}

	// We assume that different timeseries roughly have the same size (no huge outliers)
	// so we preallocate the returned slice just adding 1 extra item (+2 because a +1 is to round up).
	estimatedPartialReqs := (reqSizeWithoutMetadata / maxSize) + 2
	partialReqs := make([]*WriteRequest, 0, estimatedPartialReqs)

	// Split timeseries into partial write requests.
	nextReq, nextReqSize := newPartialReq()
	nextReqTimeseriesStart := 0
	nextReqTimeseriesLength := 0

	for i := 0; i < len(req.Timeseries); i++ {
		seriesSize := req.Timeseries[i].Size()

		// Check if the next partial request is full (or close to be full), and so it's time to finalize it and create a new one.
		// If the next partial request doesn't have any timeseries yet, we add the series anyway, in order to avoid an infinite loop
		// if a single timeseries is bigger than the limit.
		if nextReqSize+seriesSize > maxSize && nextReqTimeseriesLength > 0 {
			// Finalize the next partial request.
			nextReq.Timeseries = req.Timeseries[nextReqTimeseriesStart : nextReqTimeseriesStart+nextReqTimeseriesLength]
			partialReqs = append(partialReqs, nextReq)

			// Initialize a new partial request.
			nextReq, nextReqSize = newPartialReq()
			nextReqTimeseriesStart = i
			nextReqTimeseriesLength = 0
		}

		// Add the current series to next partial request.
		nextReqSize += seriesSize + 1 + sovMimir(uint64(seriesSize)) // Math copied from Size().
		nextReqTimeseriesLength++
	}

	if nextReqTimeseriesLength > 0 {
		// Finalize the last partial request.
		nextReq.Timeseries = req.Timeseries[nextReqTimeseriesStart : nextReqTimeseriesStart+nextReqTimeseriesLength]
		partialReqs = append(partialReqs, nextReq)
	}

	return partialReqs
}

func splitMetadataByMaxMarshalSize(req *WriteRequest, reqSize, maxSize int) []*WriteRequest {
	if len(req.Metadata) == 0 {
		return nil
	}

	newPartialReq := func() (*WriteRequest, int) {
		r := &WriteRequest{
			Source:              req.Source,
			SkipLabelValidation: req.SkipLabelValidation,
		}
		return r, r.Size()
	}

	// The partial requests returned by this function will not contain any Timeseries,
	// so we first compute the request size without it.
	reqSizeWithoutTimeseries := reqSize - req.TimeseriesSize()
	if reqSizeWithoutTimeseries <= maxSize {
		partialReq, _ := newPartialReq()
		partialReq.Metadata = req.Metadata
		return []*WriteRequest{partialReq}
	}

	// We assume that different metadata roughly have the same size (no huge outliers)
	// so we preallocate the returned slice just adding 1 extra item (+2 because a +1 is to round up).
	estimatedPartialReqs := (reqSizeWithoutTimeseries / maxSize) + 2
	partialReqs := make([]*WriteRequest, 0, estimatedPartialReqs)

	// Split metadata into partial write requests.
	nextReq, nextReqSize := newPartialReq()
	nextReqMetadataStart := 0
	nextReqMetadataLength := 0

	for i := 0; i < len(req.Metadata); i++ {
		metadataSize := req.Metadata[i].Size()

		// Check if the next partial request is full (or close to be full), and so it's time to finalize it and create a new one.
		// If the next partial request doesn't have any metadata yet, we add the metadata anyway, in order to avoid an infinite loop
		// if a single metadata is bigger than the limit.
		if nextReqSize+metadataSize > maxSize && nextReqMetadataLength > 0 {
			// Finalize the next partial request.
			nextReq.Metadata = req.Metadata[nextReqMetadataStart : nextReqMetadataStart+nextReqMetadataLength]
			partialReqs = append(partialReqs, nextReq)

			// Initialize a new partial request.
			nextReq, nextReqSize = newPartialReq()
			nextReqMetadataStart = i
			nextReqMetadataLength = 0
		}

		// Add the current metadata to next partial request.
		nextReqSize += metadataSize + 1 + sovMimir(uint64(metadataSize)) // Math copied from Size().
		nextReqMetadataLength++
	}

	if nextReqMetadataLength > 0 {
		// Finalize the last partial request.
		nextReq.Metadata = req.Metadata[nextReqMetadataStart : nextReqMetadataStart+nextReqMetadataLength]
		partialReqs = append(partialReqs, nextReq)
	}

	return partialReqs
}
