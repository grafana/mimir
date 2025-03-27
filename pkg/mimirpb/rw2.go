package mimirpb

import "slices"

func (m *WriteRequestV2) FastMarshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.FastMarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *WriteRequestV2) FastMarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.SkipLabelCountValidation {
		i--
		if m.SkipLabelCountValidation {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x3e
		i--
		dAtA[i] = 0xc8
	}
	if m.SkipLabelValidation {
		i--
		if m.SkipLabelValidation {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x3e
		i--
		dAtA[i] = 0xc0
	}
	if len(m.Timeseries) > 0 {
		for iNdEx := len(m.Timeseries) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Timeseries[iNdEx].FastMarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMimir(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x2a
		}
	}
	if len(m.Symbols) > 0 {
		for iNdEx := len(m.Symbols) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.Symbols[iNdEx])
			copy(dAtA[i:], m.Symbols[iNdEx])
			i = encodeVarintMimir(dAtA, i, uint64(len(m.Symbols[iNdEx])))
			i--
			dAtA[i] = 0x22
		}
	}
	if m.Source != 0 {
		i = encodeVarintMimir(dAtA, i, uint64(m.Source))
		i--
		dAtA[i] = 0x10
	}
	return len(dAtA) - i, nil
}

func (m *TimeSeriesV2) FastMarshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.FastMarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TimeSeriesV2) FastMarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.CreatedTimestamp != 0 {
		i = encodeVarintMimir(dAtA, i, uint64(m.CreatedTimestamp))
		i--
		dAtA[i] = 0x30
	}
	{
		size, err := m.Metadata.MarshalToSizedBuffer(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarintMimir(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x2a
	if len(m.Exemplars) > 0 {
		for iNdEx := len(m.Exemplars) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Exemplars[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMimir(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x22
		}
	}
	if len(m.Histograms) > 0 {
		for iNdEx := len(m.Histograms) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Histograms[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMimir(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	if len(m.Samples) > 0 {
		for iNdEx := len(m.Samples) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Samples[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMimir(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if len(m.LabelsRefs) > 0 {
		// This is the trick: encode the varints in reverse order to make it easier
		// to do it in place. Then reverse the whole thing.

		// Raw generated version:
		/*dAtA3 := make([]byte, len(m.LabelsRefs)*10)
		var j2 int
		for _, num := range m.LabelsRefs {
			for num >= 1<<7 {
				dAtA3[j2] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j2++
			}
			dAtA3[j2] = uint8(num)
			j2++
		}
		i -= j2
		copy(dAtA[i:], dAtA3[:j2])*/

		// Modified version:
		var j2 int
		start := i
		for _, num := range m.LabelsRefs {
			for num >= 1<<7 {
				dAtA[i-1] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				i--
				j2++
			}
			dAtA[i-1] = uint8(num)
			i--
			j2++
		}
		slices.Reverse(dAtA[i:start])

		i = encodeVarintMimir(dAtA, i, uint64(j2))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}
