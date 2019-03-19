package com.bkpasa.kafkastream.domain.model;

public class Alert {

    private Long id;
    private Long eventId;
    private Long marketId;

    private String description;
    private Boolean enabled;
    private AlertType alertType;

    private Alert() {

    }
    public Long getId() {
        return id;
    }

    public Long getEventId() {
        return eventId;
    }

    public Long getMarketId() {
        return marketId;
    }

    public String getDescription() {
        return description;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public AlertType getAlertType() {
        return alertType;
    }

    public static class Builder {
        private Long id;
        private Long eventId;
        private Long marketId;
        private String description;
        private Boolean enabled;
        private AlertType alertType;

        public Builder id(Long id) {
            this.id = id;
            return this;
        }

        public Builder eventId(Long eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder marketId(Long marketId) {
            this.marketId = marketId;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder enabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder alertType(AlertType alertType) {
            this.alertType = alertType;
            return this;
        }

        public Alert build() {
            return new Alert(this);
        }
    }

    private Alert(Builder builder) {
        this.id = builder.id;
        this.eventId = builder.eventId;
        this.marketId = builder.marketId;
        this.description = builder.description;
        this.enabled = builder.enabled;
        this.alertType = builder.alertType;
    }

    @Override
    public String toString() {
        return "Alert [id=" + id + ", eventId=" + eventId + ", marketId=" + marketId + ", description=" + description
                + ", enabled=" + enabled + ", alertType=" + alertType + "]";
    }

}
