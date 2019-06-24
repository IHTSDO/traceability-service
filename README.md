# Traceability Service

This is a standalone service to recieve and consume SNOMED CT content authoring events and expose them over a REST API.

Authoring events are recieved via JMS and MySQL is used as the datastore.

## Use
### Retrieve authoring activity on a branch
GET http://localhost:8080/activities?onBranch=MAIN/DENTISTRY
```json
{
    "content": [
        {
            "id": 5184,
            "user": {
                "username": "kkewley"
            },
            "activityType": "CONTENT_CHANGE",
            "branch": {
                "branchPath": "MAIN/DENTISTRY/DENTISTRY-23"
            },
            "highestPromotedBranch": {
                "branchPath": "MAIN/DENTISTRY"
            },
            "commitComment": "snowowl updating concept Body structure (body structure)",
            "commitDate": "2016-09-02T13:13:53Z",
            "conceptChanges": [
                {
                    "conceptId": "123037004",
                    "componentChanges": [
                        {
                            "componentId": "3314290011",
                            "componentType": "DESCRIPTION",
                            "componentSubType": "SYNONYM_DESCRIPTION",
                            "changeType": "CREATE"
                        }
                    ]
                }
            ]
        }
    ]
}
```

### Retrieve authoring activity against a concept across all branches
GET http://localhost:8080/activities?conceptId=123037004

### Restore Traceability Messages From Logs
Command line utility to restore traceability messages from log files by sending them through the JMS broker again. This may be needed if for example the JMS broker runs out of disk space.

This should be run from `/opt/authoring-traceability-service/` on the same machine as the traceability service being fixed so that the correct JMS settings and permissions are used automatically. This will not affect the normal running of the service. No downtime is required.

First copy the traceability logs which are known to have been lost into a temp directory then run:
```bash
java -jar app.jar --loadLogs lost-logs-directory --server.port=9090 
```
The name of the log files and the number of lines sent to the JMS queue will be logged. This instance may also consume the messages from JMS and write them into the database. Just kill the process once done. 
