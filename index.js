  const Q = fn => {
      try {
          return fn();
      } catch {}
  };

  const isNullish = x => x === null || x === undefined;

  async function bestEffortFetch(input, init) {
      try {
          // 1. Perform the initial fetch request
          const originalResponse = await fetch(input, init);

          // If there is no body to read (e.g., 204 No Content, HEAD requests), return as-is
          if (!originalResponse.body) {
              return originalResponse;
          }

          const originalReader = Q(() => originalResponse.body.getReader());

          // 2. Create a resilient wrapper stream
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

                      // Cleanly close the controller instead of calling controller.error()
                      Q(() => controller.close());
                  }
              },
              cancel(reason) {
                  // Ensure the underlying reader is canceled if the consumer aborts early
                  Q(() => originalReader.cancel(reason).catch(() => {}));
              },
          });

          // 3. Return a new Response cloning the original metadata but swapping the body
          return new Response(resilientStream, {
              status: originalResponse.status,
              statusText: originalResponse.statusText,
              headers: originalResponse.headers,
          });
      } catch (initialFetchError) {
          return new Response(String(initialFetchError));
      }
  }
