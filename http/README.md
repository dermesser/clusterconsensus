# `http` transport/encoding

`clusterconsensus/http` implements transports and encoding for clusterconsensus to run over JSON and HTTP. This
is not super efficient, but easy to use and debug.

## Patterns and types

* `POST /_clusterc/<clusterId>/prepare` with a body of `{"instance": InstanceNumber, "master": {"addr": Address}}`
    * Response is `{"acceptedInstance": InstanceNumber, "err": Error}`
* `POST /_clusterc/<clusterId>/accept` with a body of `{"instance": InstanceNumber, "seq": SequenceNumber, "chg": ["chg1", "chg2"...]}`
    * Response is `{"accepted": Accepted, "err": Error}`
* `POST /_clusterc/<clusterId>/addmember` with a body of `{"instance": InstanceNumber, "seq": SequenceNumber, "mem": {"addr": Address}}`
    * Response is `{"fail": Failed, "err": Error}`
* `POST /_clusterc/<clusterId>/rmmember` with a body of `{"instance": InstanceNumber, "seq": SequenceNumber, "mem": {"addr": Address}}`
    * Response is `{"fail": Failed, "err": Error}`
* `POST /_clusterc/<clusterId>/start` with a body of `{"instance": InstanceNumber, "seq": SequenceNumber, "cluster": ClusterId,
   "self": {"addr": Address}, "master": {"addr": Address}, "participants": [{"addr": Address}, ...], "snap": "..."}`
    * Response is `{"fail": Failed, "err": Error}`
* `POST /_clusterc/<clusterId>/submit` with a body of `{"chg": ["chg1", "chg2", ...]}`
    * Response is `{"fail": Failed, "err": Error}`

...where

* `<clusterId>` is a string describing the cluster that the call should go to,
* `InstanceNumber` and `SequenceNumber` are integers,
* `Failed` and `Accepted` are booleans,
* `Error` is `{"code": ErrorCode, "err": ErrorString}`, 
    * `ErrorCode` is one of the `ERR_*` consts from `clusterconsensus/errors.go`
    * `ErrorString` is a free-form string
* and `Address` is an HTTP URL.


