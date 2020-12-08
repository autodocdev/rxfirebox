package br.com.autodoc.rxfirebox.database;


import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;

import org.jetbrains.annotations.NotNull;

import io.reactivex.MaybeEmitter;
import io.reactivex.MaybeOnSubscribe;
import io.reactivex.functions.Function;

public class ListMaybeValueOnSubscribe<T> implements MaybeOnSubscribe<T> {

    private final Query mQuery;
    private final Function<DataSnapshot, T> mMarshaller;
    private final boolean mEnableRealTimeListener;

    public ListMaybeValueOnSubscribe(Query query, Function<DataSnapshot, T> marshaller,
                                     boolean enableRealTimeListener) {
        mQuery = query;
        mMarshaller = marshaller;
        mEnableRealTimeListener = enableRealTimeListener;

    }

    @Override
    public void subscribe(@NotNull MaybeEmitter<T> e) throws Exception {
        ValueEventListener listener = new RxSingleValueListener<>(e, mMarshaller, mEnableRealTimeListener);
        e.setCancellable(() -> mQuery.removeEventListener(listener));

        if (mEnableRealTimeListener) {
            mQuery.addValueEventListener(listener);
            return;
        }

        mQuery.addListenerForSingleValueEvent(listener);
    }


    private static class RxSingleValueListener<T> implements ValueEventListener {

        private final MaybeEmitter<T> subscriber;
        private final Function<DataSnapshot, T> marshaller;
        private final boolean enableRealTimeListener;

        RxSingleValueListener(MaybeEmitter<T> subscriber, Function<DataSnapshot, T> marshaller,
                              boolean enableRealTimeListener) {
            this.subscriber = subscriber;
            this.marshaller = marshaller;
            this.enableRealTimeListener = enableRealTimeListener;
        }

        @Override
        public void onDataChange(@NotNull DataSnapshot dataSnapshot) {

            try {
                if (null != marshaller.apply(dataSnapshot))
                    subscriber.onSuccess(marshaller.apply(dataSnapshot));
            } catch (Exception e) {
                subscriber.onError(e);
            }

            if (!enableRealTimeListener) {
                subscriber.onComplete();
            }

        }

        @Override
        public void onCancelled(DatabaseError databaseError) {
            subscriber.onError(databaseError.toException());
        }
    }
}
