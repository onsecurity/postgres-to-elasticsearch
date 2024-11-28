FROM node:22-alpine
WORKDIR /app
COPY ["package.json", "package-lock.json", "/app/"]
RUN npm ci

COPY ["./", "/app/"]
ENTRYPOINT ["/usr/local/bin/node", "index.js"]
