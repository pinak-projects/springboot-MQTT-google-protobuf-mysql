package org.pinak.sub.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Device {

	private long id;
	private String clientId;
	private int connAck;
	private long startTime;
	private long endTime;

}