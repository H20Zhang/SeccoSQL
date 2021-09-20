#!/usr/bin/env bash
rsync -rvz --progress ./target/scala-2.11/*.jar ./script/* ./script/upload/* itsc:/users/itsc/s880006/secco/testing/Secco