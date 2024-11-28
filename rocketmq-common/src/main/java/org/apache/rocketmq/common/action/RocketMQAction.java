package org.apache.rocketmq.common.action;

import org.apache.rocketmq.common.resource.ResourceType;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQAction {

    int value();

    ResourceType resource() default ResourceType.UNKNOWN;

    Action[] action();
}
