#!/bin/bash

set -e

echo "replacing tags of structs..."

gomodifytags -file api.pb.go -field User -struct ClientInfo -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Client -struct ClientInfo -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Presence -struct PresenceResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field NumClients -struct PresenceStatsResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field NumUsers -struct PresenceStatsResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Offset -struct HistoryResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Epoch -struct HistoryResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Publications -struct HistoryResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Version -struct HistorySnapshotResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Channel -struct HistorySnapshotResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Epoch -struct HistorySnapshotResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Offset -struct HistorySnapshotResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field CreatedAtMs -struct HistorySnapshotResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Checksum -struct HistorySnapshotResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Publications -struct HistorySnapshotResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Total -struct HistoryRestoreResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Inserted -struct HistoryRestoreResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field AlreadyPresent -struct HistoryRestoreResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Offset -struct HistoryRestoreResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Epoch -struct HistoryRestoreResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Complete -struct HistoryRestoreResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Uid -struct NodeResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Name -struct NodeResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Version -struct NodeResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field NumClients -struct NodeResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field NumUsers -struct NodeResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field NumChannels -struct NodeResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field NumSubs -struct NodeResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Channels -struct ChannelsResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Connections -struct ConnectionsResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Statuses -struct GetUserStatusResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Items -struct DeviceListResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Items -struct DeviceTopicListResult -all -w -remove-options json=omitempty >/dev/null
gomodifytags -file api.pb.go -field Items -struct UserTopicListResult -all -w -remove-options json=omitempty >/dev/null
