[![CircleCI](https://circleci.com/gh/elasticio/splitter-component/tree/master.svg?style=svg)](https://circleci.com/gh/elasticio/splitter-component/tree/master)
# splitter-component
Splitter is the basic component for the [elastic.io platform](http://www.elastic.io).

## Description
The Splitter processes income messages containing multiple elements that might have to be processed in different ways. The Splitter emits out the composite message into individual messages, each containing data related to one item.

## Actions
### Split Message By Array
**This action is deprecated, please use Split on JSONata Expression instead.**

Splits a message into multiple messages using a given separator. The separator is treated as a path to a property inside the message. A message is split when a property is an array and emitted are multiple messages. Otherwise the original message is emitted.

For example, we have a message:

```
{
    "users": [
        {
            "name": "John"
        },
        {
            "name": "Mike"
        }
    ]
}
```

The splitting expression is "users", action will return output:
```
{
    "name": "John"
}

{
    "name": "Mike"
}
```
*Notes:*

- *When splitting expression refers to an object splitter returns this object;*
- *When splitting expression contains primitive value like ```users:"John"``` or array of primitives like ```users:["John", "Mike", "Anna"]``` splitter emits error.*

### Split on JSONata Expression

This component takes the incoming message body and applies the configured JSONata tranformation on it. The evaluated transformation must be an array. This array is split and emitted into multiple messages.

For example, given the following message:

```
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

and the JSONata expression `Phone.{type: number}`, an object constructor, the action will return output:
```
{
    "home": "0203 544 1234"
}

{
    "office": "01962 001234"
}

{
    "mobile": "077 7700 1234"
}
```
*Notes:*

- *If the evaluated array contains primitive values like ```users:["John", "Mike", "Anna"]```, the splitter emits error.*

#### List of Expected Config fields
```Split Property``` - use this field to choose a separator.

### Re-assemble Messages 

Inverse of the split action: Given a stream of incoming messages a sum message is generated.
Has 3 different behaviour variants:
* Only specify group size and no delay timer. A message is emitted once the group size is reached for the given group. Should less message then the given group size arrive, then the group is silently discarded.
* Only specify delay timer and no group size. All incomming messages count towards the delay timer. Once no more message is received in this time frame there will be a emitted message for each group.
* Specify both group size and delay timer. Groups that have reached their limit are emitted directly. Beyond that the action behaves as specifed in the line before.

Supported:
* Messages can be re-ordered in the flow
* If messages are re-delivered as a result of the platform's at once delivery guarantee, does not trigger false positives
* Messages from one original message can be interleaved with messages from another original message
(e.g. Two overlapping webhook calls arrive and the flow has components where parallel processing > 1.)

Limitations:
* All groups must have one or more messages. (i.e. No groups of size 0).
Can't do re-grouping when a split is done on an empty array. (i.e. No empty for each pattern supported).
* All messages must arrive within the same container lifetime.
If at any point there is more than a 15 second gap in messages, then the group will be silently discarded.
* The group is dropped if there are any unexpected restarts to the container.
* In case only a groupSize is given and no delay timer is specified. The size of the group must be known by all group members.
* In case of using the delay timer. Messages are only emitted when all parts arrive. Emitting a message only when the first part arrives isn't supported.
* The delay timer can not exceed 40,000 milliseconds. If more than this maximum is given, then this maximum will be used instead.

#### List of Expected Config fields
```groupId``` - Globally unique id for the group to distinguish it from other groups. This value needs to be the same for all messages in a group.

```messageId``` - Id for a message to distinguish it from other messages in the group.
Must be unique per group but does not have to be globally unique. This value needs to be different for all messages in a group.

```messageData``` - Data from individual messages can be inserted here in form of an object. This object is then inserted into an array which is available in the message emitted for this group.

```groupSize``` - Number of messages in the group.

```Delay timer (in ms)``` - Time the process waits when no incoming messages before emiting (Max 40,000 milliseconds)

## Known limitations (common for the component)
None.

## Documentation links
More information and some examples can be found here: [Splitter documentation](https://www.elastic.io/connectors/splitter-integration/).
