package mimirpb

import (
	encoding_binary "encoding/binary"
	"fmt"
	"io"
	"math"
)

func UnmarshalWriteRequest(m *WriteRequest, dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: WriteRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: WriteRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timeseries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Timeseries = append(m.Timeseries, PreallocTimeseries{})
			m.Timeseries[len(m.Timeseries)-1].skipUnmarshalingExemplars = m.skipUnmarshalingExemplars
			{
				dAtA := dAtA[iNdEx:postIndex]
				p := &m.Timeseries[len(m.Timeseries)-1]
				if TimeseriesUnmarshalCachingEnabled {
					p.marshalledData = dAtA
				}
				p.TimeSeries = TimeseriesFromPool()
				p.TimeSeries.SkipUnmarshalingExemplars = p.skipUnmarshalingExemplars
				{
					dAtA := dAtA
					m := p.TimeSeries
					l := len(dAtA)
					iNdEx := 0
					for iNdEx < l {
						preIndex := iNdEx
						var wire uint64
						for shift := uint(0); ; shift += 7 {
							if shift >= 64 {
								return ErrIntOverflowMimir
							}
							if iNdEx >= l {
								return io.ErrUnexpectedEOF
							}
							b := dAtA[iNdEx]
							iNdEx++
							wire |= uint64(b&0x7F) << shift
							if b < 0x80 {
								break
							}
						}
						fieldNum := int32(wire >> 3)
						wireType := int(wire & 0x7)
						if wireType == 4 {
							return fmt.Errorf("proto: TimeSeries: wiretype end group for non-group")
						}
						if fieldNum <= 0 {
							return fmt.Errorf("proto: TimeSeries: illegal tag %d (wire type %d)", fieldNum, wire)
						}
						switch fieldNum {
						case 1:
							if wireType != 2 {
								return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
							}
							var msglen int
							for shift := uint(0); ; shift += 7 {
								if shift >= 64 {
									return ErrIntOverflowMimir
								}
								if iNdEx >= l {
									return io.ErrUnexpectedEOF
								}
								b := dAtA[iNdEx]
								iNdEx++
								msglen |= int(b&0x7F) << shift
								if b < 0x80 {
									break
								}
							}
							if msglen < 0 {
								return ErrInvalidLengthMimir
							}
							postIndex := iNdEx + msglen
							if postIndex < 0 {
								return ErrInvalidLengthMimir
							}
							if postIndex > l {
								return io.ErrUnexpectedEOF
							}
							m.Labels = append(m.Labels, LabelAdapter{})
							{
								dAtA := dAtA[iNdEx:postIndex]
								bs := &m.Labels[len(m.Labels)-1]
								l := len(dAtA)
								iNdEx := 0
								for iNdEx < l {
									preIndex := iNdEx
									var wire uint64
									for shift := uint(0); ; shift += 7 {
										if shift >= 64 {
											return ErrIntOverflowMimir
										}
										if iNdEx >= l {
											return io.ErrUnexpectedEOF
										}
										b := dAtA[iNdEx]
										iNdEx++
										wire |= uint64(b&0x7F) << shift
										if b < 0x80 {
											break
										}
									}
									fieldNum := int32(wire >> 3)
									wireType := int(wire & 0x7)
									if wireType == 4 {
										return fmt.Errorf("proto: LabelPair: wiretype end group for non-group")
									}
									if fieldNum <= 0 {
										return fmt.Errorf("proto: LabelPair: illegal tag %d (wire type %d)", fieldNum, wire)
									}
									switch fieldNum {
									case 1:
										if wireType != 2 {
											return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
										}
										var byteLen int
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											byteLen |= int(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										if byteLen < 0 {
											return ErrInvalidLengthMimir
										}
										postIndex := iNdEx + byteLen
										if postIndex < 0 {
											return ErrInvalidLengthMimir
										}
										if postIndex > l {
											return io.ErrUnexpectedEOF
										}
										bs.Name = yoloString(dAtA[iNdEx:postIndex])
										iNdEx = postIndex
									case 2:
										if wireType != 2 {
											return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
										}
										var byteLen int
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											byteLen |= int(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										if byteLen < 0 {
											return ErrInvalidLengthMimir
										}
										postIndex := iNdEx + byteLen
										if postIndex < 0 {
											return ErrInvalidLengthMimir
										}
										if postIndex > l {
											return io.ErrUnexpectedEOF
										}
										bs.Value = yoloString(dAtA[iNdEx:postIndex])
										iNdEx = postIndex
									default:
										iNdEx = preIndex
										skippy, err := skipMimir(dAtA[iNdEx:])
										if err != nil {
											return err
										}
										if skippy < 0 {
											return ErrInvalidLengthMimir
										}
										if (iNdEx + skippy) < 0 {
											return ErrInvalidLengthMimir
										}
										if (iNdEx + skippy) > l {
											return io.ErrUnexpectedEOF
										}
										iNdEx += skippy
									}
								}
								if iNdEx > l {
									return io.ErrUnexpectedEOF
								}
							}
							iNdEx = postIndex
						case 2:
							if wireType != 2 {
								return fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
							}
							var msglen int
							for shift := uint(0); ; shift += 7 {
								if shift >= 64 {
									return ErrIntOverflowMimir
								}
								if iNdEx >= l {
									return io.ErrUnexpectedEOF
								}
								b := dAtA[iNdEx]
								iNdEx++
								msglen |= int(b&0x7F) << shift
								if b < 0x80 {
									break
								}
							}
							if msglen < 0 {
								return ErrInvalidLengthMimir
							}
							postIndex := iNdEx + msglen
							if postIndex < 0 {
								return ErrInvalidLengthMimir
							}
							if postIndex > l {
								return io.ErrUnexpectedEOF
							}
							m.Samples = append(m.Samples, Sample{})
							{
								dAtA := dAtA[iNdEx:postIndex]
								m := &m.Samples[len(m.Samples)-1]
								l := len(dAtA)
								iNdEx := 0
								for iNdEx < l {
									preIndex := iNdEx
									var wire uint64
									for shift := uint(0); ; shift += 7 {
										if shift >= 64 {
											return ErrIntOverflowMimir
										}
										if iNdEx >= l {
											return io.ErrUnexpectedEOF
										}
										b := dAtA[iNdEx]
										iNdEx++
										wire |= uint64(b&0x7F) << shift
										if b < 0x80 {
											break
										}
									}
									fieldNum := int32(wire >> 3)
									wireType := int(wire & 0x7)
									if wireType == 4 {
										return fmt.Errorf("proto: Sample: wiretype end group for non-group")
									}
									if fieldNum <= 0 {
										return fmt.Errorf("proto: Sample: illegal tag %d (wire type %d)", fieldNum, wire)
									}
									switch fieldNum {
									case 1:
										if wireType != 1 {
											return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
										}
										var v uint64
										if (iNdEx + 8) > l {
											return io.ErrUnexpectedEOF
										}
										v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
										iNdEx += 8
										m.Value = float64(math.Float64frombits(v))
									case 2:
										if wireType != 0 {
											return fmt.Errorf("proto: wrong wireType = %d for field TimestampMs", wireType)
										}
										m.TimestampMs = 0
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											m.TimestampMs |= int64(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
									default:
										iNdEx = preIndex
										skippy, err := skipMimir(dAtA[iNdEx:])
										if err != nil {
											return err
										}
										if skippy < 0 {
											return ErrInvalidLengthMimir
										}
										if (iNdEx + skippy) < 0 {
											return ErrInvalidLengthMimir
										}
										if (iNdEx + skippy) > l {
											return io.ErrUnexpectedEOF
										}
										iNdEx += skippy
									}
								}
								if iNdEx > l {
									return io.ErrUnexpectedEOF
								}
							}
							iNdEx = postIndex
						case 3:
							if wireType != 2 {
								return fmt.Errorf("proto: wrong wireType = %d for field Exemplars", wireType)
							}
							var msglen int
							for shift := uint(0); ; shift += 7 {
								if shift >= 64 {
									return ErrIntOverflowMimir
								}
								if iNdEx >= l {
									return io.ErrUnexpectedEOF
								}
								b := dAtA[iNdEx]
								iNdEx++
								msglen |= int(b&0x7F) << shift
								if b < 0x80 {
									break
								}
							}
							if msglen < 0 {
								return ErrInvalidLengthMimir
							}
							postIndex := iNdEx + msglen
							if postIndex < 0 {
								return ErrInvalidLengthMimir
							}
							if postIndex > l {
								return io.ErrUnexpectedEOF
							}
							if !m.SkipUnmarshalingExemplars {
								m.Exemplars = append(m.Exemplars, Exemplar{})
								{
									dAtA := dAtA[iNdEx:postIndex]
									m := &m.Exemplars[len(m.Exemplars)-1]
									l := len(dAtA)
									iNdEx := 0
									for iNdEx < l {
										preIndex := iNdEx
										var wire uint64
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											wire |= uint64(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										fieldNum := int32(wire >> 3)
										wireType := int(wire & 0x7)
										if wireType == 4 {
											return fmt.Errorf("proto: Exemplar: wiretype end group for non-group")
										}
										if fieldNum <= 0 {
											return fmt.Errorf("proto: Exemplar: illegal tag %d (wire type %d)", fieldNum, wire)
										}
										switch fieldNum {
										case 1:
											if wireType != 2 {
												return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
											}
											var msglen int
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												msglen |= int(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
											if msglen < 0 {
												return ErrInvalidLengthMimir
											}
											postIndex := iNdEx + msglen
											if postIndex < 0 {
												return ErrInvalidLengthMimir
											}
											if postIndex > l {
												return io.ErrUnexpectedEOF
											}
											m.Labels = append(m.Labels, LabelAdapter{})
											{
												dAtA := dAtA[iNdEx:postIndex]
												bs := &m.Labels[len(m.Labels)-1]
												l := len(dAtA)
												iNdEx := 0
												for iNdEx < l {
													preIndex := iNdEx
													var wire uint64
													for shift := uint(0); ; shift += 7 {
														if shift >= 64 {
															return ErrIntOverflowMimir
														}
														if iNdEx >= l {
															return io.ErrUnexpectedEOF
														}
														b := dAtA[iNdEx]
														iNdEx++
														wire |= uint64(b&0x7F) << shift
														if b < 0x80 {
															break
														}
													}
													fieldNum := int32(wire >> 3)
													wireType := int(wire & 0x7)
													if wireType == 4 {
														return fmt.Errorf("proto: LabelPair: wiretype end group for non-group")
													}
													if fieldNum <= 0 {
														return fmt.Errorf("proto: LabelPair: illegal tag %d (wire type %d)", fieldNum, wire)
													}
													switch fieldNum {
													case 1:
														if wireType != 2 {
															return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
														}
														var byteLen int
														for shift := uint(0); ; shift += 7 {
															if shift >= 64 {
																return ErrIntOverflowMimir
															}
															if iNdEx >= l {
																return io.ErrUnexpectedEOF
															}
															b := dAtA[iNdEx]
															iNdEx++
															byteLen |= int(b&0x7F) << shift
															if b < 0x80 {
																break
															}
														}
														if byteLen < 0 {
															return ErrInvalidLengthMimir
														}
														postIndex := iNdEx + byteLen
														if postIndex < 0 {
															return ErrInvalidLengthMimir
														}
														if postIndex > l {
															return io.ErrUnexpectedEOF
														}
														bs.Name = yoloString(dAtA[iNdEx:postIndex])
														iNdEx = postIndex
													case 2:
														if wireType != 2 {
															return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
														}
														var byteLen int
														for shift := uint(0); ; shift += 7 {
															if shift >= 64 {
																return ErrIntOverflowMimir
															}
															if iNdEx >= l {
																return io.ErrUnexpectedEOF
															}
															b := dAtA[iNdEx]
															iNdEx++
															byteLen |= int(b&0x7F) << shift
															if b < 0x80 {
																break
															}
														}
														if byteLen < 0 {
															return ErrInvalidLengthMimir
														}
														postIndex := iNdEx + byteLen
														if postIndex < 0 {
															return ErrInvalidLengthMimir
														}
														if postIndex > l {
															return io.ErrUnexpectedEOF
														}
														bs.Value = yoloString(dAtA[iNdEx:postIndex])
														iNdEx = postIndex
													default:
														iNdEx = preIndex
														skippy, err := skipMimir(dAtA[iNdEx:])
														if err != nil {
															return err
														}
														if skippy < 0 {
															return ErrInvalidLengthMimir
														}
														if (iNdEx + skippy) < 0 {
															return ErrInvalidLengthMimir
														}
														if (iNdEx + skippy) > l {
															return io.ErrUnexpectedEOF
														}
														iNdEx += skippy
													}
												}
												if iNdEx > l {
													return io.ErrUnexpectedEOF
												}
											}
											iNdEx = postIndex
										case 2:
											if wireType != 1 {
												return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
											}
											var v uint64
											if (iNdEx + 8) > l {
												return io.ErrUnexpectedEOF
											}
											v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
											iNdEx += 8
											m.Value = float64(math.Float64frombits(v))
										case 3:
											if wireType != 0 {
												return fmt.Errorf("proto: wrong wireType = %d for field TimestampMs", wireType)
											}
											m.TimestampMs = 0
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												m.TimestampMs |= int64(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
										default:
											iNdEx = preIndex
											skippy, err := skipMimir(dAtA[iNdEx:])
											if err != nil {
												return err
											}
											if skippy < 0 {
												return ErrInvalidLengthMimir
											}
											if (iNdEx + skippy) < 0 {
												return ErrInvalidLengthMimir
											}
											if (iNdEx + skippy) > l {
												return io.ErrUnexpectedEOF
											}
											iNdEx += skippy
										}
									}
									if iNdEx > l {
										return io.ErrUnexpectedEOF
									}
								}
							}
							iNdEx = postIndex
						case 4:
							if wireType != 2 {
								return fmt.Errorf("proto: wrong wireType = %d for field Histograms", wireType)
							}
							var msglen int
							for shift := uint(0); ; shift += 7 {
								if shift >= 64 {
									return ErrIntOverflowMimir
								}
								if iNdEx >= l {
									return io.ErrUnexpectedEOF
								}
								b := dAtA[iNdEx]
								iNdEx++
								msglen |= int(b&0x7F) << shift
								if b < 0x80 {
									break
								}
							}
							if msglen < 0 {
								return ErrInvalidLengthMimir
							}
							postIndex := iNdEx + msglen
							if postIndex < 0 {
								return ErrInvalidLengthMimir
							}
							if postIndex > l {
								return io.ErrUnexpectedEOF
							}
							m.Histograms = append(m.Histograms, Histogram{})
							{
								dAtA := dAtA[iNdEx:postIndex]
								m := &m.Histograms[len(m.Histograms)-1]
								l := len(dAtA)
								iNdEx := 0
								for iNdEx < l {
									preIndex := iNdEx
									var wire uint64
									for shift := uint(0); ; shift += 7 {
										if shift >= 64 {
											return ErrIntOverflowMimir
										}
										if iNdEx >= l {
											return io.ErrUnexpectedEOF
										}
										b := dAtA[iNdEx]
										iNdEx++
										wire |= uint64(b&0x7F) << shift
										if b < 0x80 {
											break
										}
									}
									fieldNum := int32(wire >> 3)
									wireType := int(wire & 0x7)
									if wireType == 4 {
										return fmt.Errorf("proto: Histogram: wiretype end group for non-group")
									}
									if fieldNum <= 0 {
										return fmt.Errorf("proto: Histogram: illegal tag %d (wire type %d)", fieldNum, wire)
									}
									switch fieldNum {
									case 1:
										if wireType != 0 {
											return fmt.Errorf("proto: wrong wireType = %d for field CountInt", wireType)
										}
										var v uint64
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											v |= uint64(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										m.Count = &Histogram_CountInt{v}
									case 2:
										if wireType != 1 {
											return fmt.Errorf("proto: wrong wireType = %d for field CountFloat", wireType)
										}
										var v uint64
										if (iNdEx + 8) > l {
											return io.ErrUnexpectedEOF
										}
										v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
										iNdEx += 8
										m.Count = &Histogram_CountFloat{float64(math.Float64frombits(v))}
									case 3:
										if wireType != 1 {
											return fmt.Errorf("proto: wrong wireType = %d for field Sum", wireType)
										}
										var v uint64
										if (iNdEx + 8) > l {
											return io.ErrUnexpectedEOF
										}
										v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
										iNdEx += 8
										m.Sum = float64(math.Float64frombits(v))
									case 4:
										if wireType != 0 {
											return fmt.Errorf("proto: wrong wireType = %d for field Schema", wireType)
										}
										var v int32
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											v |= int32(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
										m.Schema = v
									case 5:
										if wireType != 1 {
											return fmt.Errorf("proto: wrong wireType = %d for field ZeroThreshold", wireType)
										}
										var v uint64
										if (iNdEx + 8) > l {
											return io.ErrUnexpectedEOF
										}
										v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
										iNdEx += 8
										m.ZeroThreshold = float64(math.Float64frombits(v))
									case 6:
										if wireType != 0 {
											return fmt.Errorf("proto: wrong wireType = %d for field ZeroCountInt", wireType)
										}
										var v uint64
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											v |= uint64(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										m.ZeroCount = &Histogram_ZeroCountInt{v}
									case 7:
										if wireType != 1 {
											return fmt.Errorf("proto: wrong wireType = %d for field ZeroCountFloat", wireType)
										}
										var v uint64
										if (iNdEx + 8) > l {
											return io.ErrUnexpectedEOF
										}
										v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
										iNdEx += 8
										m.ZeroCount = &Histogram_ZeroCountFloat{float64(math.Float64frombits(v))}
									case 8:
										if wireType != 2 {
											return fmt.Errorf("proto: wrong wireType = %d for field NegativeSpans", wireType)
										}
										var msglen int
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											msglen |= int(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										if msglen < 0 {
											return ErrInvalidLengthMimir
										}
										postIndex := iNdEx + msglen
										if postIndex < 0 {
											return ErrInvalidLengthMimir
										}
										if postIndex > l {
											return io.ErrUnexpectedEOF
										}
										m.NegativeSpans = append(m.NegativeSpans, BucketSpan{})
										{
											dAtA := dAtA[iNdEx:postIndex]
											m := &m.NegativeSpans[len(m.NegativeSpans)-1]
											l := len(dAtA)
											iNdEx := 0
											for iNdEx < l {
												preIndex := iNdEx
												var wire uint64
												for shift := uint(0); ; shift += 7 {
													if shift >= 64 {
														return ErrIntOverflowMimir
													}
													if iNdEx >= l {
														return io.ErrUnexpectedEOF
													}
													b := dAtA[iNdEx]
													iNdEx++
													wire |= uint64(b&0x7F) << shift
													if b < 0x80 {
														break
													}
												}
												fieldNum := int32(wire >> 3)
												wireType := int(wire & 0x7)
												if wireType == 4 {
													return fmt.Errorf("proto: BucketSpan: wiretype end group for non-group")
												}
												if fieldNum <= 0 {
													return fmt.Errorf("proto: BucketSpan: illegal tag %d (wire type %d)", fieldNum, wire)
												}
												switch fieldNum {
												case 1:
													if wireType != 0 {
														return fmt.Errorf("proto: wrong wireType = %d for field Offset", wireType)
													}
													var v int32
													for shift := uint(0); ; shift += 7 {
														if shift >= 64 {
															return ErrIntOverflowMimir
														}
														if iNdEx >= l {
															return io.ErrUnexpectedEOF
														}
														b := dAtA[iNdEx]
														iNdEx++
														v |= int32(b&0x7F) << shift
														if b < 0x80 {
															break
														}
													}
													v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
													m.Offset = v
												case 2:
													if wireType != 0 {
														return fmt.Errorf("proto: wrong wireType = %d for field Length", wireType)
													}
													m.Length = 0
													for shift := uint(0); ; shift += 7 {
														if shift >= 64 {
															return ErrIntOverflowMimir
														}
														if iNdEx >= l {
															return io.ErrUnexpectedEOF
														}
														b := dAtA[iNdEx]
														iNdEx++
														m.Length |= uint32(b&0x7F) << shift
														if b < 0x80 {
															break
														}
													}
												default:
													iNdEx = preIndex
													skippy, err := skipMimir(dAtA[iNdEx:])
													if err != nil {
														return err
													}
													if skippy < 0 {
														return ErrInvalidLengthMimir
													}
													if (iNdEx + skippy) < 0 {
														return ErrInvalidLengthMimir
													}
													if (iNdEx + skippy) > l {
														return io.ErrUnexpectedEOF
													}
													iNdEx += skippy
												}
											}
											if iNdEx > l {
												return io.ErrUnexpectedEOF
											}
										}
										iNdEx = postIndex
									case 9:
										if wireType == 0 {
											var v uint64
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												v |= uint64(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
											v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
											m.NegativeDeltas = append(m.NegativeDeltas, int64(v))
										} else if wireType == 2 {
											var packedLen int
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												packedLen |= int(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
											if packedLen < 0 {
												return ErrInvalidLengthMimir
											}
											postIndex := iNdEx + packedLen
											if postIndex < 0 {
												return ErrInvalidLengthMimir
											}
											if postIndex > l {
												return io.ErrUnexpectedEOF
											}
											var elementCount int
											var count int
											for _, integer := range dAtA[iNdEx:postIndex] {
												if integer < 128 {
													count++
												}
											}
											elementCount = count
											if elementCount != 0 && len(m.NegativeDeltas) == 0 {
												m.NegativeDeltas = make([]int64, 0, elementCount)
											}
											for iNdEx < postIndex {
												var v uint64
												for shift := uint(0); ; shift += 7 {
													if shift >= 64 {
														return ErrIntOverflowMimir
													}
													if iNdEx >= l {
														return io.ErrUnexpectedEOF
													}
													b := dAtA[iNdEx]
													iNdEx++
													v |= uint64(b&0x7F) << shift
													if b < 0x80 {
														break
													}
												}
												v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
												m.NegativeDeltas = append(m.NegativeDeltas, int64(v))
											}
										} else {
											return fmt.Errorf("proto: wrong wireType = %d for field NegativeDeltas", wireType)
										}
									case 10:
										if wireType == 1 {
											var v uint64
											if (iNdEx + 8) > l {
												return io.ErrUnexpectedEOF
											}
											v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
											iNdEx += 8
											v2 := float64(math.Float64frombits(v))
											m.NegativeCounts = append(m.NegativeCounts, v2)
										} else if wireType == 2 {
											var packedLen int
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												packedLen |= int(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
											if packedLen < 0 {
												return ErrInvalidLengthMimir
											}
											postIndex := iNdEx + packedLen
											if postIndex < 0 {
												return ErrInvalidLengthMimir
											}
											if postIndex > l {
												return io.ErrUnexpectedEOF
											}
											var elementCount int
											elementCount = packedLen / 8
											if elementCount != 0 && len(m.NegativeCounts) == 0 {
												m.NegativeCounts = make([]float64, 0, elementCount)
											}
											for iNdEx < postIndex {
												var v uint64
												if (iNdEx + 8) > l {
													return io.ErrUnexpectedEOF
												}
												v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
												iNdEx += 8
												v2 := float64(math.Float64frombits(v))
												m.NegativeCounts = append(m.NegativeCounts, v2)
											}
										} else {
											return fmt.Errorf("proto: wrong wireType = %d for field NegativeCounts", wireType)
										}
									case 11:
										if wireType != 2 {
											return fmt.Errorf("proto: wrong wireType = %d for field PositiveSpans", wireType)
										}
										var msglen int
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											msglen |= int(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
										if msglen < 0 {
											return ErrInvalidLengthMimir
										}
										postIndex := iNdEx + msglen
										if postIndex < 0 {
											return ErrInvalidLengthMimir
										}
										if postIndex > l {
											return io.ErrUnexpectedEOF
										}
										m.PositiveSpans = append(m.PositiveSpans, BucketSpan{})
										{
											dAtA := dAtA[iNdEx:postIndex]
											m := &m.PositiveSpans[len(m.PositiveSpans)-1]
											l := len(dAtA)
											iNdEx := 0
											for iNdEx < l {
												preIndex := iNdEx
												var wire uint64
												for shift := uint(0); ; shift += 7 {
													if shift >= 64 {
														return ErrIntOverflowMimir
													}
													if iNdEx >= l {
														return io.ErrUnexpectedEOF
													}
													b := dAtA[iNdEx]
													iNdEx++
													wire |= uint64(b&0x7F) << shift
													if b < 0x80 {
														break
													}
												}
												fieldNum := int32(wire >> 3)
												wireType := int(wire & 0x7)
												if wireType == 4 {
													return fmt.Errorf("proto: BucketSpan: wiretype end group for non-group")
												}
												if fieldNum <= 0 {
													return fmt.Errorf("proto: BucketSpan: illegal tag %d (wire type %d)", fieldNum, wire)
												}
												switch fieldNum {
												case 1:
													if wireType != 0 {
														return fmt.Errorf("proto: wrong wireType = %d for field Offset", wireType)
													}
													var v int32
													for shift := uint(0); ; shift += 7 {
														if shift >= 64 {
															return ErrIntOverflowMimir
														}
														if iNdEx >= l {
															return io.ErrUnexpectedEOF
														}
														b := dAtA[iNdEx]
														iNdEx++
														v |= int32(b&0x7F) << shift
														if b < 0x80 {
															break
														}
													}
													v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
													m.Offset = v
												case 2:
													if wireType != 0 {
														return fmt.Errorf("proto: wrong wireType = %d for field Length", wireType)
													}
													m.Length = 0
													for shift := uint(0); ; shift += 7 {
														if shift >= 64 {
															return ErrIntOverflowMimir
														}
														if iNdEx >= l {
															return io.ErrUnexpectedEOF
														}
														b := dAtA[iNdEx]
														iNdEx++
														m.Length |= uint32(b&0x7F) << shift
														if b < 0x80 {
															break
														}
													}
												default:
													iNdEx = preIndex
													skippy, err := skipMimir(dAtA[iNdEx:])
													if err != nil {
														return err
													}
													if skippy < 0 {
														return ErrInvalidLengthMimir
													}
													if (iNdEx + skippy) < 0 {
														return ErrInvalidLengthMimir
													}
													if (iNdEx + skippy) > l {
														return io.ErrUnexpectedEOF
													}
													iNdEx += skippy
												}
											}
											if iNdEx > l {
												return io.ErrUnexpectedEOF
											}
										}
										iNdEx = postIndex
									case 12:
										if wireType == 0 {
											var v uint64
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												v |= uint64(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
											v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
											m.PositiveDeltas = append(m.PositiveDeltas, int64(v))
										} else if wireType == 2 {
											var packedLen int
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												packedLen |= int(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
											if packedLen < 0 {
												return ErrInvalidLengthMimir
											}
											postIndex := iNdEx + packedLen
											if postIndex < 0 {
												return ErrInvalidLengthMimir
											}
											if postIndex > l {
												return io.ErrUnexpectedEOF
											}
											var elementCount int
											var count int
											for _, integer := range dAtA[iNdEx:postIndex] {
												if integer < 128 {
													count++
												}
											}
											elementCount = count
											if elementCount != 0 && len(m.PositiveDeltas) == 0 {
												m.PositiveDeltas = make([]int64, 0, elementCount)
											}
											for iNdEx < postIndex {
												var v uint64
												for shift := uint(0); ; shift += 7 {
													if shift >= 64 {
														return ErrIntOverflowMimir
													}
													if iNdEx >= l {
														return io.ErrUnexpectedEOF
													}
													b := dAtA[iNdEx]
													iNdEx++
													v |= uint64(b&0x7F) << shift
													if b < 0x80 {
														break
													}
												}
												v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
												m.PositiveDeltas = append(m.PositiveDeltas, int64(v))
											}
										} else {
											return fmt.Errorf("proto: wrong wireType = %d for field PositiveDeltas", wireType)
										}
									case 13:
										if wireType == 1 {
											var v uint64
											if (iNdEx + 8) > l {
												return io.ErrUnexpectedEOF
											}
											v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
											iNdEx += 8
											v2 := float64(math.Float64frombits(v))
											m.PositiveCounts = append(m.PositiveCounts, v2)
										} else if wireType == 2 {
											var packedLen int
											for shift := uint(0); ; shift += 7 {
												if shift >= 64 {
													return ErrIntOverflowMimir
												}
												if iNdEx >= l {
													return io.ErrUnexpectedEOF
												}
												b := dAtA[iNdEx]
												iNdEx++
												packedLen |= int(b&0x7F) << shift
												if b < 0x80 {
													break
												}
											}
											if packedLen < 0 {
												return ErrInvalidLengthMimir
											}
											postIndex := iNdEx + packedLen
											if postIndex < 0 {
												return ErrInvalidLengthMimir
											}
											if postIndex > l {
												return io.ErrUnexpectedEOF
											}
											var elementCount int
											elementCount = packedLen / 8
											if elementCount != 0 && len(m.PositiveCounts) == 0 {
												m.PositiveCounts = make([]float64, 0, elementCount)
											}
											for iNdEx < postIndex {
												var v uint64
												if (iNdEx + 8) > l {
													return io.ErrUnexpectedEOF
												}
												v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
												iNdEx += 8
												v2 := float64(math.Float64frombits(v))
												m.PositiveCounts = append(m.PositiveCounts, v2)
											}
										} else {
											return fmt.Errorf("proto: wrong wireType = %d for field PositiveCounts", wireType)
										}
									case 14:
										if wireType != 0 {
											return fmt.Errorf("proto: wrong wireType = %d for field ResetHint", wireType)
										}
										m.ResetHint = 0
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											m.ResetHint |= Histogram_ResetHint(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
									case 15:
										if wireType != 0 {
											return fmt.Errorf("proto: wrong wireType = %d for field Timestamp", wireType)
										}
										m.Timestamp = 0
										for shift := uint(0); ; shift += 7 {
											if shift >= 64 {
												return ErrIntOverflowMimir
											}
											if iNdEx >= l {
												return io.ErrUnexpectedEOF
											}
											b := dAtA[iNdEx]
											iNdEx++
											m.Timestamp |= int64(b&0x7F) << shift
											if b < 0x80 {
												break
											}
										}
									default:
										iNdEx = preIndex
										skippy, err := skipMimir(dAtA[iNdEx:])
										if err != nil {
											return err
										}
										if skippy < 0 {
											return ErrInvalidLengthMimir
										}
										if (iNdEx + skippy) < 0 {
											return ErrInvalidLengthMimir
										}
										if (iNdEx + skippy) > l {
											return io.ErrUnexpectedEOF
										}
										iNdEx += skippy
									}
								}
								if iNdEx > l {
									return io.ErrUnexpectedEOF
								}
							}
							iNdEx = postIndex
						default:
							iNdEx = preIndex
							skippy, err := skipMimir(dAtA[iNdEx:])
							if err != nil {
								return err
							}
							if skippy < 0 {
								return ErrInvalidLengthMimir
							}
							if (iNdEx + skippy) < 0 {
								return ErrInvalidLengthMimir
							}
							if (iNdEx + skippy) > l {
								return io.ErrUnexpectedEOF
							}
							iNdEx += skippy
						}
					}
					if iNdEx > l {
						return io.ErrUnexpectedEOF
					}
				}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Source", wireType)
			}
			m.Source = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Source |= WriteRequest_SourceEnum(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Metadata", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Metadata = append(m.Metadata, &MetricMetadata{})
			{
				dAtA := dAtA[iNdEx:postIndex]
				m := m.Metadata[len(m.Metadata)-1]
				l := len(dAtA)
				iNdEx := 0
				for iNdEx < l {
					preIndex := iNdEx
					var wire uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowMimir
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						wire |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					fieldNum := int32(wire >> 3)
					wireType := int(wire & 0x7)
					if wireType == 4 {
						return fmt.Errorf("proto: MetricMetadata: wiretype end group for non-group")
					}
					if fieldNum <= 0 {
						return fmt.Errorf("proto: MetricMetadata: illegal tag %d (wire type %d)", fieldNum, wire)
					}
					switch fieldNum {
					case 1:
						if wireType != 0 {
							return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
						}
						m.Type = 0
						for shift := uint(0); ; shift += 7 {
							if shift >= 64 {
								return ErrIntOverflowMimir
							}
							if iNdEx >= l {
								return io.ErrUnexpectedEOF
							}
							b := dAtA[iNdEx]
							iNdEx++
							m.Type |= MetricMetadata_MetricType(b&0x7F) << shift
							if b < 0x80 {
								break
							}
						}
					case 2:
						if wireType != 2 {
							return fmt.Errorf("proto: wrong wireType = %d for field MetricFamilyName", wireType)
						}
						var stringLen uint64
						for shift := uint(0); ; shift += 7 {
							if shift >= 64 {
								return ErrIntOverflowMimir
							}
							if iNdEx >= l {
								return io.ErrUnexpectedEOF
							}
							b := dAtA[iNdEx]
							iNdEx++
							stringLen |= uint64(b&0x7F) << shift
							if b < 0x80 {
								break
							}
						}
						intStringLen := int(stringLen)
						if intStringLen < 0 {
							return ErrInvalidLengthMimir
						}
						postIndex := iNdEx + intStringLen
						if postIndex < 0 {
							return ErrInvalidLengthMimir
						}
						if postIndex > l {
							return io.ErrUnexpectedEOF
						}
						m.MetricFamilyName = string(dAtA[iNdEx:postIndex])
						iNdEx = postIndex
					case 4:
						if wireType != 2 {
							return fmt.Errorf("proto: wrong wireType = %d for field Help", wireType)
						}
						var stringLen uint64
						for shift := uint(0); ; shift += 7 {
							if shift >= 64 {
								return ErrIntOverflowMimir
							}
							if iNdEx >= l {
								return io.ErrUnexpectedEOF
							}
							b := dAtA[iNdEx]
							iNdEx++
							stringLen |= uint64(b&0x7F) << shift
							if b < 0x80 {
								break
							}
						}
						intStringLen := int(stringLen)
						if intStringLen < 0 {
							return ErrInvalidLengthMimir
						}
						postIndex := iNdEx + intStringLen
						if postIndex < 0 {
							return ErrInvalidLengthMimir
						}
						if postIndex > l {
							return io.ErrUnexpectedEOF
						}
						m.Help = string(dAtA[iNdEx:postIndex])
						iNdEx = postIndex
					case 5:
						if wireType != 2 {
							return fmt.Errorf("proto: wrong wireType = %d for field Unit", wireType)
						}
						var stringLen uint64
						for shift := uint(0); ; shift += 7 {
							if shift >= 64 {
								return ErrIntOverflowMimir
							}
							if iNdEx >= l {
								return io.ErrUnexpectedEOF
							}
							b := dAtA[iNdEx]
							iNdEx++
							stringLen |= uint64(b&0x7F) << shift
							if b < 0x80 {
								break
							}
						}
						intStringLen := int(stringLen)
						if intStringLen < 0 {
							return ErrInvalidLengthMimir
						}
						postIndex := iNdEx + intStringLen
						if postIndex < 0 {
							return ErrInvalidLengthMimir
						}
						if postIndex > l {
							return io.ErrUnexpectedEOF
						}
						m.Unit = string(dAtA[iNdEx:postIndex])
						iNdEx = postIndex
					default:
						iNdEx = preIndex
						skippy, err := skipMimir(dAtA[iNdEx:])
						if err != nil {
							return err
						}
						if skippy < 0 {
							return ErrInvalidLengthMimir
						}
						if (iNdEx + skippy) < 0 {
							return ErrInvalidLengthMimir
						}
						if (iNdEx + skippy) > l {
							return io.ErrUnexpectedEOF
						}
						iNdEx += skippy
					}
				}
				if iNdEx > l {
					return io.ErrUnexpectedEOF
				}
			}
			iNdEx = postIndex
		case 1000:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SkipLabelNameValidation", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.SkipLabelNameValidation = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}
	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
