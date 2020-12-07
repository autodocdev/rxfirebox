package br.com.autodoc.rxfirebox.database;


import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;

import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.functions.Function;

public class ListValueOnSubscribe<T> implements FlowableOnSubscribe<T> {

    private Query mQuery;
    private Function<DataSnapshot, T> mMarshaller;
    private final boolean mEnableRealTimeListener;


    public ListValueOnSubscribe(Query query, Function<DataSnapshot, T> marshaller,
                                boolean enableRealTimeListener) {
        mQuery = query;
        mMarshaller = marshaller;
        mEnableRealTimeListener = enableRealTimeListener;

    }

    @Override
    public void subscribe(FlowableEmitter<T> e) throws Exception {
        ValueEventListener listener = new RxSingleValueListener<>(e, mMarshaller);
        e.setCancellable(() -> mQuery.removeEventListener(listener));

        if (mEnableRealTimeListener) {
            mQuery.addValueEventListener(listener);
        }
        mQuery.addListenerForSingleValueEvent(listener);
    }


    private static class RxSingleValueListener<T> implements ValueEventListener {

        private final FlowableEmitter<T> subscriber;
        private final Function<DataSnapshot, T> marshaller;

        RxSingleValueListener(FlowableEmitter<T> subscriber, Function<DataSnapshot, T> marshaller) {
            this.subscriber = subscriber;
            this.marshaller = marshaller;
        }

        @Override
        public void onDataChange(DataSnapshot dataSnapshot) {

            try {
                if (null != marshaller.apply(dataSnapshot))
                    subscriber.onNext(marshaller.apply(dataSnapshot));
            } catch (Exception e) {
                subscriber.onError(e);
            }

            subscriber.onComplete();

        }

        @Override
        public void onCancelled(DatabaseError databaseError) {
            subscriber.onError(databaseError.toException());
        }
    }
}
