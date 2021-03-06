/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378.assign8;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class EventSubtypeStatisticsKey extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"EventSubtypeStatisticsKey\",\"namespace\":\"com.refactorlabs.cs378.assign8\",\"fields\":[{\"name\":\"session_type\",\"type\":\"string\",\"default\":\"any\"},{\"name\":\"event_subtype\",\"type\":\"string\",\"default\":\"any\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence session_type;
  @Deprecated public java.lang.CharSequence event_subtype;

  /**
   * Default constructor.
   */
  public EventSubtypeStatisticsKey() {}

  /**
   * All-args constructor.
   */
  public EventSubtypeStatisticsKey(java.lang.CharSequence session_type, java.lang.CharSequence event_subtype) {
    this.session_type = session_type;
    this.event_subtype = event_subtype;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return session_type;
    case 1: return event_subtype;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: session_type = (java.lang.CharSequence)value$; break;
    case 1: event_subtype = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'session_type' field.
   */
  public java.lang.CharSequence getSessionType() {
    return session_type;
  }

  /**
   * Sets the value of the 'session_type' field.
   * @param value the value to set.
   */
  public void setSessionType(java.lang.CharSequence value) {
    this.session_type = value;
  }

  /**
   * Gets the value of the 'event_subtype' field.
   */
  public java.lang.CharSequence getEventSubtype() {
    return event_subtype;
  }

  /**
   * Sets the value of the 'event_subtype' field.
   * @param value the value to set.
   */
  public void setEventSubtype(java.lang.CharSequence value) {
    this.event_subtype = value;
  }

  /** Creates a new EventSubtypeStatisticsKey RecordBuilder */
  public static com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder newBuilder() {
    return new com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder();
  }
  
  /** Creates a new EventSubtypeStatisticsKey RecordBuilder by copying an existing Builder */
  public static com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder newBuilder(com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder other) {
    return new com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder(other);
  }
  
  /** Creates a new EventSubtypeStatisticsKey RecordBuilder by copying an existing EventSubtypeStatisticsKey instance */
  public static com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder newBuilder(com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey other) {
    return new com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder(other);
  }
  
  /**
   * RecordBuilder for EventSubtypeStatisticsKey instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<EventSubtypeStatisticsKey>
    implements org.apache.avro.data.RecordBuilder<EventSubtypeStatisticsKey> {

    private java.lang.CharSequence session_type;
    private java.lang.CharSequence event_subtype;

    /** Creates a new Builder */
    private Builder() {
      super(com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing EventSubtypeStatisticsKey instance */
    private Builder(com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey other) {
            super(com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.SCHEMA$);
      if (isValidValue(fields()[0], other.session_type)) {
        this.session_type = data().deepCopy(fields()[0].schema(), other.session_type);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.event_subtype)) {
        this.event_subtype = data().deepCopy(fields()[1].schema(), other.event_subtype);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'session_type' field */
    public java.lang.CharSequence getSessionType() {
      return session_type;
    }
    
    /** Sets the value of the 'session_type' field */
    public com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder setSessionType(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.session_type = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'session_type' field has been set */
    public boolean hasSessionType() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'session_type' field */
    public com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder clearSessionType() {
      session_type = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'event_subtype' field */
    public java.lang.CharSequence getEventSubtype() {
      return event_subtype;
    }
    
    /** Sets the value of the 'event_subtype' field */
    public com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder setEventSubtype(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.event_subtype = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'event_subtype' field has been set */
    public boolean hasEventSubtype() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'event_subtype' field */
    public com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey.Builder clearEventSubtype() {
      event_subtype = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public EventSubtypeStatisticsKey build() {
      try {
        EventSubtypeStatisticsKey record = new EventSubtypeStatisticsKey();
        record.session_type = fieldSetFlags()[0] ? this.session_type : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.event_subtype = fieldSetFlags()[1] ? this.event_subtype : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
