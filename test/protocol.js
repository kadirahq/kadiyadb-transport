module.exports = require("protobufjs").newBuilder({})['import']({
    "package": "test",
    "messages": [
        {
            "name": "Test",
            "fields": [
                {
                    "rule": "optional",
                    "type": "uint32",
                    "name": "n",
                    "id": 1
                }
            ]
        }
    ]
}).build();