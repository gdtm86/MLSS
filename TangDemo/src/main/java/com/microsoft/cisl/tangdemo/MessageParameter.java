package com.microsoft.cisl.tangdemo;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;


@NamedParameter(doc = "A named parameter for the message to print out.", default_value = "Hello!")
public class MessageParameter implements Name<String> {
}
