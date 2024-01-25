import http from "k6/http";
import { check } from "k6";

// Function to generate a random integer between min and max (inclusive)
function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Function to generate random strings, a simple version of faker
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
  // A number specifying the number of VUs to run concurrently.
  vus: 10,
  // A string specifying the total duration of the test run.
  duration: "30s",
};

export default function () {
  let payload = {
    streams: [],
  };

  for (let i = 0; i < getRandomInt(1, 10); i++) {
    let entry = {
      stream: {},
      values: [],
    };

    for (let j = 0; j < getRandomInt(1, 10); j++) {
      entry.stream[randomString(8)] = randomString(5); // Simulating faker.Username() and faker.Letters()
    }

    for (let k = 0; k < getRandomInt(1, 10); k++) {
      entry.values.push([
        `${Date.now()}`, // Simulating time.Now().UnixNano() in JavaScript
        randomString(10), // Simulating faker.Sentence()
      ]);
    }

    payload.streams.push(entry);
  }

  // Convert payload to JSON
  let body = JSON.stringify(payload);

  // Define the HTTP request parameters
  let params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  // Send the POST request
  let res = http.put("http://localhost:6500/api/streams", body, params);

  // Check for HTTP status code 200
  check(res, {
    "is status 200": (r) => r.status === 200,
  });
}
