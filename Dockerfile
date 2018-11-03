FROM node:11
COPY src /src
WORKDIR /src
RUN npm install
ENTRYPOINT ["/usr/local/bin/node", "index.js"]
