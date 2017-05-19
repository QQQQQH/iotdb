
package com.corp.delta.sql.src.ql.parse;

import java.util.ArrayList;

public class ParseException extends Exception {

  private static final long serialVersionUID = 1L;
  private ArrayList<ParseError> errors;

  public ParseException(ArrayList<ParseError> errors) {
    super();
    this.errors = errors;
  }

  @Override
  public String getMessage() {

    StringBuilder sb = new StringBuilder();
    
    for (ParseError err : errors) {
      if (sb.length() > 0) {
        sb.append('\n');
      }
      sb.append(err.getMessage());
    }
	
    return sb.toString();
  }

}
