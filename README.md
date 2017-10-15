# ekit: Distributed Systems Lab Kit

This code aims at providing a simple abstraction of a fully connected network
to ease the implementation of distributed protocols on the Atomix Catalyst 
evend driven programming framework.

## Usage

Clone from github and install with `mvn install`.

Add the following dependency:

```
<dependency>
    <groupId>pt.haslab</groupId>
    <artifactId>ekit</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Create and join a clique as follows:

```
    Address[] addresses = ...
    int id = ...
    Transport t = ...
    ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());
        
    tc.execute(() -> {
        Clique c = new Clique(t, id, addresses);

        c.handler(..., (j,m)-> {
            // message handler
            ...
        }).onException(e->{
            // exception handler
            ...
        });

        c.open().thenRun(() -> {
            // initialization code
            ...
        });
    }).join();
```

## More information

See the Catalyst documentation: http://atomix.io/catalyst/api/latest/

## License

Copyright (C) 2017 José Orlando Pereira

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
