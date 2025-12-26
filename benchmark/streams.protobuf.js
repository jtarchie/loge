import http from "k6/http";
import { check } from "k6";

// protobuf encoding helpers for k6 (no TextEncoder available)

// Varint encoding helper
function encodeVarint(value) {
  const bytes = [];
  while (value > 127) {
    bytes.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  bytes.push(value & 0x7f);
  return bytes;
}

// Length-prefixed bytes encoding
function encodeLengthDelimited(fieldNumber, bytes) {
  const tag = (fieldNumber << 3) | 2; // wire type 2 = length-delimited
  return [...encodeVarint(tag), ...encodeVarint(bytes.length), ...bytes];
}

// String to UTF-8 bytes (k6 doesn't have TextEncoder)
function stringToBytes(str) {
  const bytes = [];
  for (let i = 0; i < str.length; i++) {
    let code = str.charCodeAt(i);
    if (code < 0x80) {
      bytes.push(code);
    } else if (code < 0x800) {
      bytes.push(0xc0 | (code >> 6));
      bytes.push(0x80 | (code & 0x3f));
    } else if (code < 0xd800 || code >= 0xe000) {
      bytes.push(0xe0 | (code >> 12));
      bytes.push(0x80 | ((code >> 6) & 0x3f));
      bytes.push(0x80 | (code & 0x3f));
    } else {
      // Surrogate pair
      i++;
      code = 0x10000 + (((code & 0x3ff) << 10) | (str.charCodeAt(i) & 0x3ff));
      bytes.push(0xf0 | (code >> 18));
      bytes.push(0x80 | ((code >> 12) & 0x3f));
      bytes.push(0x80 | ((code >> 6) & 0x3f));
      bytes.push(0x80 | (code & 0x3f));
    }
  }
  return bytes;
}

// Encode a map<string, string> field
function encodeStringMap(fieldNumber, map) {
  const result = [];
  for (const key in map) {
    if (Object.prototype.hasOwnProperty.call(map, key)) {
      const value = map[key];
      // Each map entry is a sub-message with key=1, value=2
      const keyBytes = encodeLengthDelimited(1, stringToBytes(key));
      const valueBytes = encodeLengthDelimited(2, stringToBytes(value));
      const entryBytes = [...keyBytes, ...valueBytes];
      result.push(...encodeLengthDelimited(fieldNumber, entryBytes));
    }
  }
  return result;
}

// Encode a Value message
function encodeValue(timestamp, line) {
  const timestampBytes = encodeLengthDelimited(1, stringToBytes(timestamp));
  const lineBytes = encodeLengthDelimited(2, stringToBytes(line));
  return [...timestampBytes, ...lineBytes];
}

// Encode a StreamEntry message
function encodeStreamEntry(stream, values) {
  const result = [];
  // Encode stream map (field 1)
  result.push(...encodeStringMap(1, stream));
  // Encode values (field 2)
  for (let i = 0; i < values.length; i++) {
    const valueBytes = encodeValue(values[i][0], values[i][1]);
    result.push(...encodeLengthDelimited(2, valueBytes));
  }
  return result;
}

// Encode a Payload message
function encodePayload(payload) {
  const result = [];
  for (let i = 0; i < payload.streams.length; i++) {
    const streamEntry = payload.streams[i];
    const entryBytes = encodeStreamEntry(
      streamEntry.stream,
      streamEntry.values
    );
    result.push(...encodeLengthDelimited(1, entryBytes));
  }
  return new Uint8Array(result);
}

// Function to generate a random integer between min and max (inclusive)
function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Function to generate random strings
function randomString(length) {
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

export const options = {
  vus: 50,
  duration: "30s",
};

// Generate payload once (outside the default function for better performance)
let payload = {
  streams: [],
};

for (let i = 0; i < getRandomInt(1, 10); i++) {
  let entry = {
    stream: {},
    values: [],
  };

  for (let j = 0; j < getRandomInt(1, 10); j++) {
    entry.stream[randomString(8)] = randomString(5);
  }

  for (let k = 0; k < getRandomInt(1, 10); k++) {
    entry.values.push([`${Date.now()}`, randomString(10)]);
  }

  payload.streams.push(entry);
}

// Encode the payload to protobuf binary format
let body = encodePayload(payload);

export default function () {
  let params = {
    headers: {
      "Content-Type": "application/protobuf",
    },
  };

  let res = http.post("http://localhost:6500/api/v1/push", body.buffer, params);

  check(res, {
    "is status 200": (r) => r.status === 200,
  });
}
