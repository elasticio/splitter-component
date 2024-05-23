# Splitter Component

## Table of Contents
* [Description](#description)
* [Actions](#actions)
  * [Split on JSONata Expression](#Split-on-JSONata-Expression)
  * [Re-assemble Messages](#Re-assemble-Messages)

## Description

The Splitter Component is a fundamental part of our platform. Its primary purpose is to split arrays into separate messages or to combine separate messages into one.

## Actions 
  
### Split on JSONata Expression

Splits a single message into several separate messages.

#### Configuration Fields

* **JSONata Expression** - (string, required): Enter the expression that will be evaluated as an array.

 <details><summary>Example</summary>

 You have the following incoming message:

  ```json
    {
        "FirstName": "Fred",
        "Surname": "Smith",
        "Phone": [
            {
                "type": "home",
                "number": "0203 544 1234"
            },
            {
                "type": "office",
                "number": "01962 001234"
            },
            {
                "type": "mobile",
                "number": "077 7700 1234"
            }
        ]
    }
  ```

  If the JSONata expression is set to `Phone.{type: number}`, you will get three messages:
  
  ```json
    {
        "home": "0203 544 1234"
    }
  ```
  ```json
    {
        "office": "01962 001234"
    }
  ```
  ```json
    {
        "mobile": "077 7700 1234"
    }
  ```
  </details> <br>

#### Input Metadata

None

#### Output Metadata

Each item of the array will be emitted as a separate message.

### Re-assemble Messages

Combines separate messages into one.

#### Configuration Fields

* **Behavior** - (dropdown, required): Select one of the following options:
  * `Produce Groups of Fixed Size (Don't Emit Partial Groups)` - Messages keeps collecting continuously. Once the group size is reached, the group is emitted and the new group starts collecting immediately. If the number of incoming messages for a particular group is less than the defined group size, the group will be stored in the internal storage (Maester) and proceed collecting messages into the open group.
  * `Group All Incoming Messages` - All incoming messages will be gathered until there are no more incoming messages within the specified timeframe (delay timer), at which point messages will be emitted for each group.
  * `Produce Groups of Fixed Size (Emit Partial Groups)` - Specify both group size and delay timer. Once a group is complete, that group will be emitted. If there are no more incoming messages within the specified timeframe, partially completed groups will also be emitted.
* **Emit result as array** - (checkbox, optional): If selected, `messageData` in the response object will be an array of messages without message IDs.

  <details><summary>Example with unchecked</summary>

  ```json
    {
    "groupSize": 2,
    "groupId": "test22",
    "messageData": {
        "d899b000-5455-4c7a-9781-f16203426b93": {
        "dataFromMessage": "Message1"
        },
        "bdfca2b1-7aa7-444c-916d-3a2c17fc5dd6": {
        "dataFromMessage": "Message2"
        }
    }
    }
  ```
  </details> <br>
  <details><summary>Example with checked</summary>

  ```json
    {
    "groupSize": 2,
    "groupId": "test22",
    "messageData": [
        {
        "dataFromMessage": "Message1"
        },
        {
        "dataFromMessage": "Message2"
        }
    ]
    }
  ```
  </details> <br>

#### Input Metadata

* **Unique ID to describe the group** - (string, required): A unique ID for the group to distinguish it from other groups.
* **Unique ID to describe this message** - (string, optional): An ID for a message to distinguish it from other messages in the group. Must be unique per group but does not have to be globally unique. This value needs to be different for all messages in a group. If a messageId occurs multiple times, only the messageData of the latest message will be retained. If the messageId is not defined, a random GUID will be generated and used as the messageID.
* **Message Data** - (object, optional): Data from individual messages.
  
  If `Produce Groups of Fixed Size (Don't Emit Partial Groups)` or `Produce Groups of Fixed Size (Emit Partial Groups)` is selected:
  * **Number of messages expected to be reassembled into the group** - (number, optional): The number of messages when the group is considered full.
  
  If `Group All Incoming Messages` or `Produce Groups of Fixed Size (Emit Partial Groups)` is selected:
  * **Time the process waits when no incoming messages before emitting (Default 20000 milliseconds)** - (number, optional): The time the process waits when no incoming messages before emitting. Maximum 20,000 ms.

#### Output Metadata

* **groupSize** - (number, required): The number of messages in this group.
* **groupId** - (number, required): The ID of this group.
* **messageData** - (number, required): If `Emit result as array` is selected, this will be an array of messages from previous steps; otherwise, it will be an object with keys as `Unique ID to describe this message` and values as messages from previous steps.

#### Known Limitations

* The total size of stored messages in groups should be less than 5MB; otherwise, the component will emit the group regardless of the selected behavior.
* Messages are stored in component memory during execution - "Suspending" the flow will erase them.
* With option `Produce Groups of Fixed Size (Don't Emit Partial Groups)` if group is not ready, messages will be stored inside internal storage (Maester) for up to two days