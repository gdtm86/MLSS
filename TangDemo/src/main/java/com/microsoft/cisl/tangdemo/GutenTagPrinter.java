package com.microsoft.cisl.tangdemo;

import javax.inject.Inject;

class GutenTagPrinter implements MessagePrinter {

  @Inject
  GutenTagPrinter() {
  }

  @Override
  public void printMessage() {
    System.out.println("Guten Tag!");
  }
}
