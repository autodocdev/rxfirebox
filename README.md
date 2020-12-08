[![N|Solid](http://www.autodoc.com.br/site/informativo/qr/img/logo-autodoc.png)](http://site.autodoc.com.br/)

# RxFireBox

The simple wrapper to encapsulate rxjava and firebase.

### Libraries
  - RxJava2
  - RxAndroid
  - Firebase Storage
  - Firebase Database
  - Firestore Database

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
	implementation 'com.github.autodocdev:rxfirebox:2.3.12'
}
```
Firebase
```java
public class FooFirebaseRepository extends DatabaseBox<Foo> {
    private final DatabaseReference databaseReference;
    private final DatabaseObserver databaseObserver;

    public FooFirebaseRepository(DatabaseReference databaseReference,
        DatabaseObserver databaseObserver) {
        this.databaseReference = databaseReference;
        this.databaseObserver = databaseObserver;
    }

    public Completable save(Foo foo) {
        return databaseBox.set(foo, databaseReference);
    }

    public Completable update(Foo foo) {
             Map<String, Foo> update = new HashMap<>();
             update.put(foo.getId(), foo);
             return observer.update(update, databaseReference);
        }

    public Maybe<List<Foo>> single() {
        Query query = databaseReference.orderByValue().equalTo(false);
        return observer.single(query, toList());
    }

    public Maybe<List<Foo>> list() {
            Query query = databaseReference.orderByValue().equalTo(false);
            return observer.listMaybe(query, toList());
        }

    public Flowable<List<Foo>> list() {
        Query query = databaseReference.orderByValue();
        return observer.list(query, toList());
    }
}
```
Firestore
```java
public class FooFirebaseRepository extends FirestoreBox<Foo> {
    private final FirebaseFirestore databaseReference;
    private final FirestoreObserver databaseObserver;

    public FooFirebaseRepository(DatabaseReference databaseReference,
        DatabaseObserver databaseObserver) {
        this.databaseReference = databaseReference;
        this.databaseObserver = databaseObserver;
    }

    public Completable save(Foo foo) {
      WriteBatch batch = databaseReference.batch();
      CollectionReference reference = databaseReference.collection();
      batch.set(reference.document());
      return observer.batch(batch);
    }

    public Completable save(Foo foo) {
          return observer.set(foo, databaseReference);
    }


    public Completable update(Foo foo) {
        Map<String, Foo> update = new HashMap<>();
        update.put(foo, foo);
        return observer.update(update, databaseReference);
    }

    public Maybe<List<Foo>> single() {
        Query query = databaseReference.orderByValue().equalTo(false);
        return observer.single(query, toList());
    }

    public Maybe<Foo> list() {
       Query query = databaseReference().whereEqualTo();
       return observer.single(query, toFirst());
    }

    public Flowable<List<Foo>> list() {
        Query query = databaseReference.collection();
        return observer.list(query, toList());
    }
}
```

```
Copyright 2020 Autodoc

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
