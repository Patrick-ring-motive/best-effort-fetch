  const Q = fn => {
    try {
      return fn();
    } catch {}
  };

  const isNullish = x => x === null || x === undefined;

  async function bestEffortResponse(response) {
    try {
      const originalResponse = response;
      if (!originalResponse.body) {
        return originalResponse;
      }
      const originalReader = Q(() => originalResponse.body.getReader());
      const resilientStream = new ReadableStream({
        async pull(controller) {
          try {
            const {
              done,
              value
            } = await originalReader.read();

            if (done == true || (isNullish(done) && isNullish(value))) {
              Q(() => controller.close());
            } else {
              controller.enqueue(value);
            }
          } catch (streamError) {
            // A mid-stream network drop or timeout caught here!
            console.warn(
              "Stream interrupted prematurely. Closing stream gracefully with partial data.",
              streamError,
            );

            Q(() => controller?.close?.()?.catch?.(() => {}));
          }
        },
        cancel(reason) {
          Q(() => originalReader?.cancel?.(reason)?.catch?.(() => {}));
        },
      });
      return new Response(resilientStream, {
        status: originalResponse.status,
        statusText: originalResponse.statusText,
        headers: originalResponse.headers,
      });
    } catch (e) {
      return new Response(String(e), {
        status: 500,
        statusText: String(e)
      });
    }
  }

  async function bestEffortFetch(input, init) {
    try {
      const originalResponse = await fetch(input, init);
      return await bestEffortResponse(originalResponse);
    } catch (e) {
      return new Response(String(e), {
        status: 500,
        statusText: String(e)
      });
    }
  }
  const encoder = new TextEncoder();
  const encode = encoder.encode.bind(encoder);

  async function $bytes(res) {
    const chunks = [];
    let reader;
    try {
      reader = res?.body?.getReader?.();
      if (reader) {
        while (true) {
          const {
            done,
            value
          } = await reader.read();
          if (done == true || (isNullish(done) && isNullish(value))) {
            break;
          }
          chunks.push(value);
        }
      }
    } catch (e) {
      return encode(String(e));
    } finally {
      Q(() => reader?.releaseLock?.()?.catch?.(() => {}));
    }
    return new Uint8Array(await new Blob(chunks).arrayBuffer());
  }

  const decoder = new TextDecoder();
  const decode = decoder.decode.bind(decoder);

  async function $text(res) {
    try {
      if (!res.body) {
        return res.statusText || '';
      }
      return decode(await $bytes(res));
    } catch (e) {
      return String(e);
    }
  }

  const fetchResponse = async (...args) => {
    try {
      return await bestEffortFetch(...args);
    } catch (e) {
      return new Response(String(e), {
        status: 500,
        statusText: String(e)
      });
    }
  };

  async function fetchAllWithRetry(requests, concurrency = 6) {
    const results = new Array(requests.length);
    const retries = [];

    const requestsLength = requests.length;
    for (let i = 0; i < requestsLength; i += concurrency) {
      const batch = requests.slice(i, i + concurrency);
      const settled = await Promise.all(batch.map(req => fetchResponse(req.clone())));

      for (const [j, res] of settled.entries()) {
        const idx = i + j;
        if (/^2/.test(res.status)) {
          results[idx] = res;
          Q(() => requests[idx].body.cancel());
        } else {
          retries.push(idx);
        }
      }
    }

    for (const i of retries) {
      results[i] = await fetchResponse(requests[i].url, requests[i]);
    }
    return results;
  }

  async function serializeResponse(res) {
    return {
      status: String(res?.status),
      statusText: String(res?.statusText),
      headers: res?.headers?.entries ? Object.fromEntries(res.headers.entries()) : res?.headers,
      body: await $text(res)
    };
  }

  export default {
    async fetch(request, env, ctx) {
      const payload = await request.json();
      const requests = payload.map(x => new Request(String(x.url ?? x), Object(x)));
      const responses = await fetchAllWithRetry(requests);
      const flatResponses = await Promise.all(responses.map(serializeResponse));
      return new Response(JSON.stringify(flatResponses));
    }
  };
