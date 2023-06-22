// Package names contains the ValidateTopicName function.
//
// Outside lib/kafka, don't import this package directly. Instead, import
// lib/kafka which reexports ValidateTopicName.
package names

import (
	"errors"
	"fmt"
	"regexp"
)

var reValidTopicName = regexp.MustCompile(`^[-_.a-zA-Z0-9]+$`)

// ValidateTopicName returns an error if the given topic name is invalid.
//
// Mirrors org.apache.kafka.common.internals.Topic.validate
// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L36
func ValidateTopicName(name string) error {
	if name == "" {
		return errors.New("topic name cannot be empty")
	}
	if name == "." || name == ".." {
		return errors.New(`topic name cannot be "." or ".."`)
	}
	if len(name) > 249 {
		return errors.New("topic name cannot be longer than 249 characters")
	}
	if !reValidTopicName.MatchString(name) {
		return fmt.Errorf("invalid topic name: %s (must match %s)", name, reValidTopicName)
	}
	return nil
}
