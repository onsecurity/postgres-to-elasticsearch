FROM node:12-alpine
WORKDIR /app
COPY ["package.json", "package-lock.json", "/app/"]
RUN npm ci

COPY ["src/", "/app/src/"]
ENTRYPOINT ["/usr/local/bin/node", "src/index.js"]
