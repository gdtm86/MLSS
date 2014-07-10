package com.microsoft.cisl.tangdemo;

import javax.inject.Inject;

class Greeter {

  private final MessagePrinter messagePrinter;

  @Inject
  Greeter(MessagePrinter messagePrinter) {
    this.messagePrinter = messagePrinter;
  }

  void greet() {
    messagePrinter.printMessage();
  }
}
