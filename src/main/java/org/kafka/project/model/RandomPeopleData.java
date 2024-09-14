package org.kafka.project.model;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class RandomPeopleData implements Serializable {
    private Person[] ctRoot;
}
