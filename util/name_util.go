package util

import (
	"fmt"
	v1 "io.pravega.pravega-client-go/controller/proto"
)

const (
	EpochDelimiter = ".#epoch."
)

/***
  public static String getQualifiedStreamSegmentName(String scope, String streamName, long segmentId) {
      int segmentNumber = getSegmentNumber(segmentId);
      int epoch = getEpoch(segmentId);
      StringBuilder sb = getScopedStreamNameInternal(scope, streamName);
      sb.append('/');
      sb.append(segmentNumber);
      sb.append(EPOCH_DELIMITER);
      sb.append(epoch);
      return sb.toString();
  }
*/

func getEpoch(segmentId int64) int32 {
	return int32(segmentId >> 32)
}

func GetQualifiedStreamSegmentName(segment *v1.SegmentId) string {
	segmentNumber := int32(segment.SegmentId)
	epoch := getEpoch(segment.SegmentId)
	return fmt.Sprintf("%s/%s/%v%s%v", segment.StreamInfo.Scope, segment.StreamInfo.Stream, segmentNumber, EpochDelimiter, epoch)
}
