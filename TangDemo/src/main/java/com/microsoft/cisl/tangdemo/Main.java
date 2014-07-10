package com.microsoft.cisl.tangdemo;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;

public class Main {

  public static void main(final String[] args) throws InjectionException {
//    Configuration helloConf = Tang.Factory.getTang().newConfigurationBuilder()
//        .bindImplementation(MessagePrinter.class, HelloPrinter.class)
//        .build();
//    Greeter greeter = Tang.Factory.getTang().newInjector(helloConf).getInstance(Greeter.class);
//    greeter.greet();

//    Configuration gutenTagConf = Tang.Factory.getTang().newConfigurationBuilder()
//        .bindImplementation(MessagePrinter.class, GutenTagPrinter.class)
//        .build();
//    Greeter greeter = Tang.Factory.getTang().newInjector(gutenTagConf).getInstance(Greeter.class);
//    greeter.greet();


    Configuration bonjourConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MessagePrinter.class, ConfiguredMessagePrinter.class)
        .bindNamedParameter(MessageParameter.class, "Bonjour!")
        .build();
    Greeter greeter = Tang.Factory.getTang().newInjector(bonjourConf).getInstance(Greeter.class);
    greeter.greet();
  }
}
