package loge

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Entry) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "stream":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "Stream")
				return
			}
			if z.Stream == nil {
				z.Stream = make(Stream, zb0002)
			} else if len(z.Stream) > 0 {
				for key := range z.Stream {
					delete(z.Stream, key)
				}
			}
			for zb0002 > 0 {
				zb0002--
				var za0001 string
				var za0002 string
				za0001, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "Stream")
					return
				}
				za0002, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "Stream", za0001)
					return
				}
				z.Stream[za0001] = za0002
			}
		case "values":
			var zb0003 uint32
			zb0003, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Values")
				return
			}
			if cap(z.Values) >= int(zb0003) {
				z.Values = (z.Values)[:zb0003]
			} else {
				z.Values = make(Values, zb0003)
			}
			for za0003 := range z.Values {
				var zb0004 uint32
				zb0004, err = dc.ReadArrayHeader()
				if err != nil {
					err = msgp.WrapError(err, "Values", za0003)
					return
				}
				if zb0004 != uint32(2) {
					err = msgp.ArrayError{Wanted: uint32(2), Got: zb0004}
					return
				}
				for za0004 := range z.Values[za0003] {
					z.Values[za0003][za0004], err = dc.ReadString()
					if err != nil {
						err = msgp.WrapError(err, "Values", za0003, za0004)
						return
					}
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Entry) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "stream"
	err = en.Append(0x82, 0xa6, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Stream)))
	if err != nil {
		err = msgp.WrapError(err, "Stream")
		return
	}
	for za0001, za0002 := range z.Stream {
		err = en.WriteString(za0001)
		if err != nil {
			err = msgp.WrapError(err, "Stream")
			return
		}
		err = en.WriteString(za0002)
		if err != nil {
			err = msgp.WrapError(err, "Stream", za0001)
			return
		}
	}
	// write "values"
	err = en.Append(0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Values)))
	if err != nil {
		err = msgp.WrapError(err, "Values")
		return
	}
	for za0003 := range z.Values {
		err = en.WriteArrayHeader(uint32(2))
		if err != nil {
			err = msgp.WrapError(err, "Values", za0003)
			return
		}
		for za0004 := range z.Values[za0003] {
			err = en.WriteString(z.Values[za0003][za0004])
			if err != nil {
				err = msgp.WrapError(err, "Values", za0003, za0004)
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Entry) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "stream"
	o = append(o, 0x82, 0xa6, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d)
	o = msgp.AppendMapHeader(o, uint32(len(z.Stream)))
	for za0001, za0002 := range z.Stream {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendString(o, za0002)
	}
	// string "values"
	o = append(o, 0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Values)))
	for za0003 := range z.Values {
		o = msgp.AppendArrayHeader(o, uint32(2))
		for za0004 := range z.Values[za0003] {
			o = msgp.AppendString(o, z.Values[za0003][za0004])
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Entry) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "stream":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Stream")
				return
			}
			if z.Stream == nil {
				z.Stream = make(Stream, zb0002)
			} else if len(z.Stream) > 0 {
				for key := range z.Stream {
					delete(z.Stream, key)
				}
			}
			for zb0002 > 0 {
				var za0001 string
				var za0002 string
				zb0002--
				za0001, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Stream")
					return
				}
				za0002, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Stream", za0001)
					return
				}
				z.Stream[za0001] = za0002
			}
		case "values":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Values")
				return
			}
			if cap(z.Values) >= int(zb0003) {
				z.Values = (z.Values)[:zb0003]
			} else {
				z.Values = make(Values, zb0003)
			}
			for za0003 := range z.Values {
				var zb0004 uint32
				zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Values", za0003)
					return
				}
				if zb0004 != uint32(2) {
					err = msgp.ArrayError{Wanted: uint32(2), Got: zb0004}
					return
				}
				for za0004 := range z.Values[za0003] {
					z.Values[za0003][za0004], bts, err = msgp.ReadStringBytes(bts)
					if err != nil {
						err = msgp.WrapError(err, "Values", za0003, za0004)
						return
					}
				}
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Entry) Msgsize() (s int) {
	s = 1 + 7 + msgp.MapHeaderSize
	if z.Stream != nil {
		for za0001, za0002 := range z.Stream {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.StringPrefixSize + len(za0002)
		}
	}
	s += 7 + msgp.ArrayHeaderSize
	for za0003 := range z.Values {
		s += msgp.ArrayHeaderSize
		for za0004 := range z.Values[za0003] {
			s += msgp.StringPrefixSize + len(z.Values[za0003][za0004])
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Payload) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "streams":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Streams")
				return
			}
			if cap(z.Streams) >= int(zb0002) {
				z.Streams = (z.Streams)[:zb0002]
			} else {
				z.Streams = make(Streams, zb0002)
			}
			for za0001 := range z.Streams {
				var zb0003 uint32
				zb0003, err = dc.ReadMapHeader()
				if err != nil {
					err = msgp.WrapError(err, "Streams", za0001)
					return
				}
				for zb0003 > 0 {
					zb0003--
					field, err = dc.ReadMapKeyPtr()
					if err != nil {
						err = msgp.WrapError(err, "Streams", za0001)
						return
					}
					switch msgp.UnsafeString(field) {
					case "stream":
						var zb0004 uint32
						zb0004, err = dc.ReadMapHeader()
						if err != nil {
							err = msgp.WrapError(err, "Streams", za0001, "Stream")
							return
						}
						if z.Streams[za0001].Stream == nil {
							z.Streams[za0001].Stream = make(Stream, zb0004)
						} else if len(z.Streams[za0001].Stream) > 0 {
							for key := range z.Streams[za0001].Stream {
								delete(z.Streams[za0001].Stream, key)
							}
						}
						for zb0004 > 0 {
							zb0004--
							var za0002 string
							var za0003 string
							za0002, err = dc.ReadString()
							if err != nil {
								err = msgp.WrapError(err, "Streams", za0001, "Stream")
								return
							}
							za0003, err = dc.ReadString()
							if err != nil {
								err = msgp.WrapError(err, "Streams", za0001, "Stream", za0002)
								return
							}
							z.Streams[za0001].Stream[za0002] = za0003
						}
					case "values":
						var zb0005 uint32
						zb0005, err = dc.ReadArrayHeader()
						if err != nil {
							err = msgp.WrapError(err, "Streams", za0001, "Values")
							return
						}
						if cap(z.Streams[za0001].Values) >= int(zb0005) {
							z.Streams[za0001].Values = (z.Streams[za0001].Values)[:zb0005]
						} else {
							z.Streams[za0001].Values = make(Values, zb0005)
						}
						for za0004 := range z.Streams[za0001].Values {
							var zb0006 uint32
							zb0006, err = dc.ReadArrayHeader()
							if err != nil {
								err = msgp.WrapError(err, "Streams", za0001, "Values", za0004)
								return
							}
							if zb0006 != uint32(2) {
								err = msgp.ArrayError{Wanted: uint32(2), Got: zb0006}
								return
							}
							for za0005 := range z.Streams[za0001].Values[za0004] {
								z.Streams[za0001].Values[za0004][za0005], err = dc.ReadString()
								if err != nil {
									err = msgp.WrapError(err, "Streams", za0001, "Values", za0004, za0005)
									return
								}
							}
						}
					default:
						err = dc.Skip()
						if err != nil {
							err = msgp.WrapError(err, "Streams", za0001)
							return
						}
					}
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Payload) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "streams"
	err = en.Append(0x81, 0xa7, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Streams)))
	if err != nil {
		err = msgp.WrapError(err, "Streams")
		return
	}
	for za0001 := range z.Streams {
		// map header, size 2
		// write "stream"
		err = en.Append(0x82, 0xa6, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d)
		if err != nil {
			return
		}
		err = en.WriteMapHeader(uint32(len(z.Streams[za0001].Stream)))
		if err != nil {
			err = msgp.WrapError(err, "Streams", za0001, "Stream")
			return
		}
		for za0002, za0003 := range z.Streams[za0001].Stream {
			err = en.WriteString(za0002)
			if err != nil {
				err = msgp.WrapError(err, "Streams", za0001, "Stream")
				return
			}
			err = en.WriteString(za0003)
			if err != nil {
				err = msgp.WrapError(err, "Streams", za0001, "Stream", za0002)
				return
			}
		}
		// write "values"
		err = en.Append(0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(z.Streams[za0001].Values)))
		if err != nil {
			err = msgp.WrapError(err, "Streams", za0001, "Values")
			return
		}
		for za0004 := range z.Streams[za0001].Values {
			err = en.WriteArrayHeader(uint32(2))
			if err != nil {
				err = msgp.WrapError(err, "Streams", za0001, "Values", za0004)
				return
			}
			for za0005 := range z.Streams[za0001].Values[za0004] {
				err = en.WriteString(z.Streams[za0001].Values[za0004][za0005])
				if err != nil {
					err = msgp.WrapError(err, "Streams", za0001, "Values", za0004, za0005)
					return
				}
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Payload) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "streams"
	o = append(o, 0x81, 0xa7, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Streams)))
	for za0001 := range z.Streams {
		// map header, size 2
		// string "stream"
		o = append(o, 0x82, 0xa6, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d)
		o = msgp.AppendMapHeader(o, uint32(len(z.Streams[za0001].Stream)))
		for za0002, za0003 := range z.Streams[za0001].Stream {
			o = msgp.AppendString(o, za0002)
			o = msgp.AppendString(o, za0003)
		}
		// string "values"
		o = append(o, 0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
		o = msgp.AppendArrayHeader(o, uint32(len(z.Streams[za0001].Values)))
		for za0004 := range z.Streams[za0001].Values {
			o = msgp.AppendArrayHeader(o, uint32(2))
			for za0005 := range z.Streams[za0001].Values[za0004] {
				o = msgp.AppendString(o, z.Streams[za0001].Values[za0004][za0005])
			}
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Payload) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "streams":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Streams")
				return
			}
			if cap(z.Streams) >= int(zb0002) {
				z.Streams = (z.Streams)[:zb0002]
			} else {
				z.Streams = make(Streams, zb0002)
			}
			for za0001 := range z.Streams {
				var zb0003 uint32
				zb0003, bts, err = msgp.ReadMapHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Streams", za0001)
					return
				}
				for zb0003 > 0 {
					zb0003--
					field, bts, err = msgp.ReadMapKeyZC(bts)
					if err != nil {
						err = msgp.WrapError(err, "Streams", za0001)
						return
					}
					switch msgp.UnsafeString(field) {
					case "stream":
						var zb0004 uint32
						zb0004, bts, err = msgp.ReadMapHeaderBytes(bts)
						if err != nil {
							err = msgp.WrapError(err, "Streams", za0001, "Stream")
							return
						}
						if z.Streams[za0001].Stream == nil {
							z.Streams[za0001].Stream = make(Stream, zb0004)
						} else if len(z.Streams[za0001].Stream) > 0 {
							for key := range z.Streams[za0001].Stream {
								delete(z.Streams[za0001].Stream, key)
							}
						}
						for zb0004 > 0 {
							var za0002 string
							var za0003 string
							zb0004--
							za0002, bts, err = msgp.ReadStringBytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "Streams", za0001, "Stream")
								return
							}
							za0003, bts, err = msgp.ReadStringBytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "Streams", za0001, "Stream", za0002)
								return
							}
							z.Streams[za0001].Stream[za0002] = za0003
						}
					case "values":
						var zb0005 uint32
						zb0005, bts, err = msgp.ReadArrayHeaderBytes(bts)
						if err != nil {
							err = msgp.WrapError(err, "Streams", za0001, "Values")
							return
						}
						if cap(z.Streams[za0001].Values) >= int(zb0005) {
							z.Streams[za0001].Values = (z.Streams[za0001].Values)[:zb0005]
						} else {
							z.Streams[za0001].Values = make(Values, zb0005)
						}
						for za0004 := range z.Streams[za0001].Values {
							var zb0006 uint32
							zb0006, bts, err = msgp.ReadArrayHeaderBytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "Streams", za0001, "Values", za0004)
								return
							}
							if zb0006 != uint32(2) {
								err = msgp.ArrayError{Wanted: uint32(2), Got: zb0006}
								return
							}
							for za0005 := range z.Streams[za0001].Values[za0004] {
								z.Streams[za0001].Values[za0004][za0005], bts, err = msgp.ReadStringBytes(bts)
								if err != nil {
									err = msgp.WrapError(err, "Streams", za0001, "Values", za0004, za0005)
									return
								}
							}
						}
					default:
						bts, err = msgp.Skip(bts)
						if err != nil {
							err = msgp.WrapError(err, "Streams", za0001)
							return
						}
					}
				}
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Payload) Msgsize() (s int) {
	s = 1 + 8 + msgp.ArrayHeaderSize
	for za0001 := range z.Streams {
		s += 1 + 7 + msgp.MapHeaderSize
		if z.Streams[za0001].Stream != nil {
			for za0002, za0003 := range z.Streams[za0001].Stream {
				_ = za0003
				s += msgp.StringPrefixSize + len(za0002) + msgp.StringPrefixSize + len(za0003)
			}
		}
		s += 7 + msgp.ArrayHeaderSize
		for za0004 := range z.Streams[za0001].Values {
			s += msgp.ArrayHeaderSize
			for za0005 := range z.Streams[za0001].Values[za0004] {
				s += msgp.StringPrefixSize + len(z.Streams[za0001].Values[za0004][za0005])
			}
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Stream) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0003 uint32
	zb0003, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if (*z) == nil {
		(*z) = make(Stream, zb0003)
	} else if len((*z)) > 0 {
		for key := range *z {
			delete((*z), key)
		}
	}
	for zb0003 > 0 {
		zb0003--
		var zb0001 string
		var zb0002 string
		zb0001, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		zb0002, err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		(*z)[zb0001] = zb0002
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Stream) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteMapHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0004, zb0005 := range z {
		err = en.WriteString(zb0004)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		err = en.WriteString(zb0005)
		if err != nil {
			err = msgp.WrapError(err, zb0004)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Stream) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendMapHeader(o, uint32(len(z)))
	for zb0004, zb0005 := range z {
		o = msgp.AppendString(o, zb0004)
		o = msgp.AppendString(o, zb0005)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Stream) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0003 uint32
	zb0003, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if (*z) == nil {
		(*z) = make(Stream, zb0003)
	} else if len((*z)) > 0 {
		for key := range *z {
			delete((*z), key)
		}
	}
	for zb0003 > 0 {
		var zb0001 string
		var zb0002 string
		zb0003--
		zb0001, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		zb0002, bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		(*z)[zb0001] = zb0002
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Stream) Msgsize() (s int) {
	s = msgp.MapHeaderSize
	if z != nil {
		for zb0004, zb0005 := range z {
			_ = zb0005
			s += msgp.StringPrefixSize + len(zb0004) + msgp.StringPrefixSize + len(zb0005)
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Streams) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0006 uint32
	zb0006, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0006) {
		(*z) = (*z)[:zb0006]
	} else {
		(*z) = make(Streams, zb0006)
	}
	for zb0001 := range *z {
		var field []byte
		_ = field
		var zb0007 uint32
		zb0007, err = dc.ReadMapHeader()
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		for zb0007 > 0 {
			zb0007--
			field, err = dc.ReadMapKeyPtr()
			if err != nil {
				err = msgp.WrapError(err, zb0001)
				return
			}
			switch msgp.UnsafeString(field) {
			case "stream":
				var zb0008 uint32
				zb0008, err = dc.ReadMapHeader()
				if err != nil {
					err = msgp.WrapError(err, zb0001, "Stream")
					return
				}
				if (*z)[zb0001].Stream == nil {
					(*z)[zb0001].Stream = make(Stream, zb0008)
				} else if len((*z)[zb0001].Stream) > 0 {
					for key := range (*z)[zb0001].Stream {
						delete((*z)[zb0001].Stream, key)
					}
				}
				for zb0008 > 0 {
					zb0008--
					var zb0002 string
					var zb0003 string
					zb0002, err = dc.ReadString()
					if err != nil {
						err = msgp.WrapError(err, zb0001, "Stream")
						return
					}
					zb0003, err = dc.ReadString()
					if err != nil {
						err = msgp.WrapError(err, zb0001, "Stream", zb0002)
						return
					}
					(*z)[zb0001].Stream[zb0002] = zb0003
				}
			case "values":
				var zb0009 uint32
				zb0009, err = dc.ReadArrayHeader()
				if err != nil {
					err = msgp.WrapError(err, zb0001, "Values")
					return
				}
				if cap((*z)[zb0001].Values) >= int(zb0009) {
					(*z)[zb0001].Values = ((*z)[zb0001].Values)[:zb0009]
				} else {
					(*z)[zb0001].Values = make(Values, zb0009)
				}
				for zb0004 := range (*z)[zb0001].Values {
					var zb0010 uint32
					zb0010, err = dc.ReadArrayHeader()
					if err != nil {
						err = msgp.WrapError(err, zb0001, "Values", zb0004)
						return
					}
					if zb0010 != uint32(2) {
						err = msgp.ArrayError{Wanted: uint32(2), Got: zb0010}
						return
					}
					for zb0005 := range (*z)[zb0001].Values[zb0004] {
						(*z)[zb0001].Values[zb0004][zb0005], err = dc.ReadString()
						if err != nil {
							err = msgp.WrapError(err, zb0001, "Values", zb0004, zb0005)
							return
						}
					}
				}
			default:
				err = dc.Skip()
				if err != nil {
					err = msgp.WrapError(err, zb0001)
					return
				}
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Streams) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0011 := range z {
		// map header, size 2
		// write "stream"
		err = en.Append(0x82, 0xa6, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d)
		if err != nil {
			return
		}
		err = en.WriteMapHeader(uint32(len(z[zb0011].Stream)))
		if err != nil {
			err = msgp.WrapError(err, zb0011, "Stream")
			return
		}
		for zb0012, zb0013 := range z[zb0011].Stream {
			err = en.WriteString(zb0012)
			if err != nil {
				err = msgp.WrapError(err, zb0011, "Stream")
				return
			}
			err = en.WriteString(zb0013)
			if err != nil {
				err = msgp.WrapError(err, zb0011, "Stream", zb0012)
				return
			}
		}
		// write "values"
		err = en.Append(0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(z[zb0011].Values)))
		if err != nil {
			err = msgp.WrapError(err, zb0011, "Values")
			return
		}
		for zb0014 := range z[zb0011].Values {
			err = en.WriteArrayHeader(uint32(2))
			if err != nil {
				err = msgp.WrapError(err, zb0011, "Values", zb0014)
				return
			}
			for zb0015 := range z[zb0011].Values[zb0014] {
				err = en.WriteString(z[zb0011].Values[zb0014][zb0015])
				if err != nil {
					err = msgp.WrapError(err, zb0011, "Values", zb0014, zb0015)
					return
				}
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Streams) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0011 := range z {
		// map header, size 2
		// string "stream"
		o = append(o, 0x82, 0xa6, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d)
		o = msgp.AppendMapHeader(o, uint32(len(z[zb0011].Stream)))
		for zb0012, zb0013 := range z[zb0011].Stream {
			o = msgp.AppendString(o, zb0012)
			o = msgp.AppendString(o, zb0013)
		}
		// string "values"
		o = append(o, 0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
		o = msgp.AppendArrayHeader(o, uint32(len(z[zb0011].Values)))
		for zb0014 := range z[zb0011].Values {
			o = msgp.AppendArrayHeader(o, uint32(2))
			for zb0015 := range z[zb0011].Values[zb0014] {
				o = msgp.AppendString(o, z[zb0011].Values[zb0014][zb0015])
			}
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Streams) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0006 uint32
	zb0006, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0006) {
		(*z) = (*z)[:zb0006]
	} else {
		(*z) = make(Streams, zb0006)
	}
	for zb0001 := range *z {
		var field []byte
		_ = field
		var zb0007 uint32
		zb0007, bts, err = msgp.ReadMapHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		for zb0007 > 0 {
			zb0007--
			field, bts, err = msgp.ReadMapKeyZC(bts)
			if err != nil {
				err = msgp.WrapError(err, zb0001)
				return
			}
			switch msgp.UnsafeString(field) {
			case "stream":
				var zb0008 uint32
				zb0008, bts, err = msgp.ReadMapHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, zb0001, "Stream")
					return
				}
				if (*z)[zb0001].Stream == nil {
					(*z)[zb0001].Stream = make(Stream, zb0008)
				} else if len((*z)[zb0001].Stream) > 0 {
					for key := range (*z)[zb0001].Stream {
						delete((*z)[zb0001].Stream, key)
					}
				}
				for zb0008 > 0 {
					var zb0002 string
					var zb0003 string
					zb0008--
					zb0002, bts, err = msgp.ReadStringBytes(bts)
					if err != nil {
						err = msgp.WrapError(err, zb0001, "Stream")
						return
					}
					zb0003, bts, err = msgp.ReadStringBytes(bts)
					if err != nil {
						err = msgp.WrapError(err, zb0001, "Stream", zb0002)
						return
					}
					(*z)[zb0001].Stream[zb0002] = zb0003
				}
			case "values":
				var zb0009 uint32
				zb0009, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, zb0001, "Values")
					return
				}
				if cap((*z)[zb0001].Values) >= int(zb0009) {
					(*z)[zb0001].Values = ((*z)[zb0001].Values)[:zb0009]
				} else {
					(*z)[zb0001].Values = make(Values, zb0009)
				}
				for zb0004 := range (*z)[zb0001].Values {
					var zb0010 uint32
					zb0010, bts, err = msgp.ReadArrayHeaderBytes(bts)
					if err != nil {
						err = msgp.WrapError(err, zb0001, "Values", zb0004)
						return
					}
					if zb0010 != uint32(2) {
						err = msgp.ArrayError{Wanted: uint32(2), Got: zb0010}
						return
					}
					for zb0005 := range (*z)[zb0001].Values[zb0004] {
						(*z)[zb0001].Values[zb0004][zb0005], bts, err = msgp.ReadStringBytes(bts)
						if err != nil {
							err = msgp.WrapError(err, zb0001, "Values", zb0004, zb0005)
							return
						}
					}
				}
			default:
				bts, err = msgp.Skip(bts)
				if err != nil {
					err = msgp.WrapError(err, zb0001)
					return
				}
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Streams) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0011 := range z {
		s += 1 + 7 + msgp.MapHeaderSize
		if z[zb0011].Stream != nil {
			for zb0012, zb0013 := range z[zb0011].Stream {
				_ = zb0013
				s += msgp.StringPrefixSize + len(zb0012) + msgp.StringPrefixSize + len(zb0013)
			}
		}
		s += 7 + msgp.ArrayHeaderSize
		for zb0014 := range z[zb0011].Values {
			s += msgp.ArrayHeaderSize
			for zb0015 := range z[zb0011].Values[zb0014] {
				s += msgp.StringPrefixSize + len(z[zb0011].Values[zb0014][zb0015])
			}
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Value) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0001 uint32
	zb0001, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != uint32(2) {
		err = msgp.ArrayError{Wanted: uint32(2), Got: zb0001}
		return
	}
	for za0001 := range z {
		z[za0001], err = dc.ReadString()
		if err != nil {
			err = msgp.WrapError(err, za0001)
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Value) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(2))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for za0001 := range z {
		err = en.WriteString(z[za0001])
		if err != nil {
			err = msgp.WrapError(err, za0001)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Value) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(2))
	for za0001 := range z {
		o = msgp.AppendString(o, z[za0001])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Value) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != uint32(2) {
		err = msgp.ArrayError{Wanted: uint32(2), Got: zb0001}
		return
	}
	for za0001 := range z {
		z[za0001], bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, za0001)
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Value) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for za0001 := range z {
		s += msgp.StringPrefixSize + len(z[za0001])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Values) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0003 uint32
	zb0003, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0003) {
		(*z) = (*z)[:zb0003]
	} else {
		(*z) = make(Values, zb0003)
	}
	for zb0001 := range *z {
		var zb0004 uint32
		zb0004, err = dc.ReadArrayHeader()
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		if zb0004 != uint32(2) {
			err = msgp.ArrayError{Wanted: uint32(2), Got: zb0004}
			return
		}
		for zb0002 := range (*z)[zb0001] {
			(*z)[zb0001][zb0002], err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, zb0001, zb0002)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Values) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0005 := range z {
		err = en.WriteArrayHeader(uint32(2))
		if err != nil {
			err = msgp.WrapError(err, zb0005)
			return
		}
		for zb0006 := range z[zb0005] {
			err = en.WriteString(z[zb0005][zb0006])
			if err != nil {
				err = msgp.WrapError(err, zb0005, zb0006)
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Values) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0005 := range z {
		o = msgp.AppendArrayHeader(o, uint32(2))
		for zb0006 := range z[zb0005] {
			o = msgp.AppendString(o, z[zb0005][zb0006])
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Values) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0003 uint32
	zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0003) {
		(*z) = (*z)[:zb0003]
	} else {
		(*z) = make(Values, zb0003)
	}
	for zb0001 := range *z {
		var zb0004 uint32
		zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		if zb0004 != uint32(2) {
			err = msgp.ArrayError{Wanted: uint32(2), Got: zb0004}
			return
		}
		for zb0002 := range (*z)[zb0001] {
			(*z)[zb0001][zb0002], bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, zb0001, zb0002)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Values) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0005 := range z {
		s += msgp.ArrayHeaderSize
		for zb0006 := range z[zb0005] {
			s += msgp.StringPrefixSize + len(z[zb0005][zb0006])
		}
	}
	return
}
