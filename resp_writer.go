package main

func (v Value) replyValue() []byte {
	switch v.typ {
	case "array":
		return v.replyArray()
	case "string":
		return v.replyString()
	case "error":
		return v.replyError()
	case "integer":
		return v.replyInteger()
	case "bulk":
		return v.replyBulk()
	case "null":
		return v.replyNull()
	default:
		return []byte{}
	}
}