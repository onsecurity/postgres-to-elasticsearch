FROM node:12-alpine
COPY src /src
WORKDIR /src
RUN npm install
ENTRYPOINT ["/usr/local/bin/node", "index.js"]
