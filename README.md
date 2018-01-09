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
