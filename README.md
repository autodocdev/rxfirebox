[![N|Solid](http://www.autodoc.com.br/site/informativo/qr/img/logo-autodoc.png)](http://site.autodoc.com.br/)

# RxFireBox

The simple wrapper for encapsulating rxjava and firebase.

### Libraries
  - RxJava2
  - RxAndroid
  - Firebase Storage
  - Firebase Database

### How to use

Add in build.gradle
```gradle
allprojects {
	repositories {
		maven { url 'https://jitpack.io' }
	}
}
```

Add in dependecies
```gradle
dependencies {
	compile 'com.github.autodocdev:rxfirebox:1.0.0'
}
```

```java
public class FooFirebaseRepository {
    private final DatabaseReference databaseReference;
    private final DatabaseBox databaseBox;
    private final Box<Foo> box;

    public FooFirebaseRepository(DatabaseReference databaseReference,
        DatabaseBox databaseBox,
        Box<Foo> box) {
        this.databaseReference = databaseReference;
        this.databaseBox = databaseBox;
        this.box = box;
    }

    public Completable save(Foo foo) {
        return databaseBox.set(foo, databaseReference);
    }

    public Maybe<List<Foo>> single() {
        Query query = databaseReference.orderByValue().equalTo(false);
        return databaseBox.single(query, box.toList());
    }

    public Flowable<List<Foo>> list() {
        Query query = databaseReference.orderByValue();
        return databaseBox.list(query, box.toList());
    }
}
```

```
Copyright 2017 Autodoc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
