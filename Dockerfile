FROM node:12
COPY src /src
WORKDIR /src
RUN npm install
ENTRYPOINT ["/usr/local/bin/node", "index.js"]
