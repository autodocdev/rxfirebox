package br.com.autodoc.rxfirebox.database;


import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;

import org.jetbrains.annotations.NotNull;

import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.functions.Function;

public class ListValueOnSubscribe<T> implements FlowableOnSubscribe<T> {

    private final Query mQuery;
    private final Function<DataSnapshot, T> mMarshaller;

    public ListValueOnSubscribe(Query query, Function<DataSnapshot, T> marshaller) {
        mQuery = query;
        mMarshaller = marshaller;
    }

    @Override
    public void subscribe(@NotNull FlowableEmitter<T> e) throws Exception {
        ValueEventListener listener = new RxSingleValueListener<>(e, mMarshaller);
        e.setCancellable(() -> this.mQuery.removeEventListener(listener));

        this.mQuery.addValueEventListener(listener);
    }

    private static class RxSingleValueListener<T> implements ValueEventListener {

        private final FlowableEmitter<T> subscriber;
        private final Function<DataSnapshot, T> marshaller;

        RxSingleValueListener(FlowableEmitter<T> subscriber, Function<DataSnapshot, T> marshaller) {
            this.subscriber = subscriber;
            this.marshaller = marshaller;
        }

        @Override
        public void onDataChange(@NotNull DataSnapshot dataSnapshot) {

            try {
                if (null != marshaller.apply(dataSnapshot))
                    this.subscriber.onNext(this.marshaller.apply(dataSnapshot));
            } catch (Exception e) {
                this.subscriber.onError(e);
            }

            this.subscriber.onComplete();
        }

        @Override
        public void onCancelled(DatabaseError databaseError) {
            this.subscriber.onError(databaseError.toException());
        }
    }
}
