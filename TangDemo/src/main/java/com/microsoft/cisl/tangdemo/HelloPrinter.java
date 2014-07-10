package com.microsoft.cisl.tangdemo;

import javax.inject.Inject;

class HelloPrinter implements MessagePrinter {

  @Inject
  HelloPrinter() {
  }

  @Override
  public void printMessage() {
    System.out.println("Hello!");
  }
}
