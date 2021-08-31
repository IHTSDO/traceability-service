# Traceability Service

This is a standalone service to consume SNOMED CT content authoring events and expose them over a REST API.

Authoring events are received over JMS and Elasticsearch is used as the datastore.

## Use
### Retrieve authoring activity on a branch
GET http://localhost:8080/activities?onBranch=MAIN/DENTISTRY

### Retrieve authoring activity against a concept across all branches
GET http://localhost:8080/activities?conceptId=123037004
