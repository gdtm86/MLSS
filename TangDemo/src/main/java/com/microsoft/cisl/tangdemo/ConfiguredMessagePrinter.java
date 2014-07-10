package com.microsoft.cisl.tangdemo;

import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;

class ConfiguredMessagePrinter implements MessagePrinter {
  private final String message;

  @Inject
  public ConfiguredMessagePrinter(@Parameter(MessageParameter.class) String message) {
    this.message = message;
  }


  @Override
  public void printMessage() {
    System.out.println(message);
  }
}
