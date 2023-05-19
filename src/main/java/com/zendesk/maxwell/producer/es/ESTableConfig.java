package com.zendesk.maxwell.producer.es;

public class ESTableConfig {
	private String targetName;
	private Integer numberOfShards;
	private Integer numberOfReplicas;
	private Integer maxResultWindow;
	private String[] sourceKeys;
	private String[] targetKeys;
	private String[] sourceFields;
	private String[] targetFields;

	public Integer getNumberOfShards() {
		return numberOfShards;
	}

	public void setNumberOfShards(Integer numberOfShards) {
		this.numberOfShards = numberOfShards;
	}

	public Integer getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public void setNumberOfReplicas(Integer numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
	}

	public Integer getMaxResultWindow() {
		return maxResultWindow;
	}

	public void setMaxResultWindow(Integer maxResultWindow) {
		this.maxResultWindow = maxResultWindow;
	}

	public String getTargetName() {
		return targetName;
	}

	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}

	public String[] getSourceFields() {
		return sourceFields;
	}

	public void setSourceFields(String[] sourceFields) {
		this.sourceFields = sourceFields;
	}

	public String[] getTargetFields() {
		return targetFields;
	}

	public void setTargetFields(String[] targetFields) {
		this.targetFields = targetFields;
	}

	public String[] getSourceKeys() {
		return sourceKeys;
	}

	public void setSourceKeys(String[] sourceKeys) {
		this.sourceKeys = sourceKeys;
	}

	public String[] getTargetKeys() {
		return targetKeys;
	}

	public void setTargetKeys(String[] targetKeys) {
		this.targetKeys = targetKeys;
	}
}
