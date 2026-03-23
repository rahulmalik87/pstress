FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY bld/src/pstress-ch /usr/local/bin/pstress-ch
ENTRYPOINT ["/usr/local/bin/pstress-ch"]
