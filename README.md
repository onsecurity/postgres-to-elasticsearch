# Postgres-to-elasticsearch

Getting data from PostgreSQL to Elasticsearch, originally designed for audit archiving.

Performs both historic and real-time event driven data streaming.

### Prerequisites

* Node 8+ with npm available
* Requires the database to use an incremental primary key to work correctly

## Getting Started

Review the top of `src/index.js` for the list of environment variables required.

Each environment variable comes with a comment which explains it's purpose and effect

```
cd src
npm install
ENV_VARIABLES=todo node index.js
```

## Deployment

The service may be run directly on a host machine or via docker.

The docker image can be built using:

```
cd /project/root;
sudo docker build -t mytag/name .
```
## Built With

* [Node.js](https://nodejs.org/en/)

## Versioning

Currently no versioning system

## Authors

* **Tom Lindley** - *Development* - [OnSecurity](https://www.onsecurity.co.uk/)

## License

This project is licensed under the GNUv3 License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

* Inspired by Steve Smith's article [https://developer.atlassian.com/blog/2015/02/realtime-requests-psql-elasticsearch](https://developer.atlassian.com/blog/2015/02/realtime-requests-psql-elasticsearch).
