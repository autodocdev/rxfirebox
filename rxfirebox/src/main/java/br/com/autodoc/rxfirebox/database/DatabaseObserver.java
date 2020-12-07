package br.com.autodoc.rxfirebox.database;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.Query;

import java.util.Map;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;

public class DatabaseObserver {

    private final boolean useListener;

    public DatabaseObserver() {
        useListener = false;
    }

    public DatabaseObserver(boolean useListener, boolean enableRealTimeListener) {
        this.useListener = useListener;
    }

    /**
     * Run the query once and cancel the listener
     *
     * @param query firebase query
     * @param marshaller function to convert data
     * @param <T> type of the return
     * @return return type converted in marshaller
     */
    public <T> Maybe<T> single(Query query, Function<DataSnapshot, T> marshaller) {
        return Maybe.create(new SingleValueOnSubscribe<>(query, marshaller));
    }

    /**
     * Run the query and not cancel the listener
     *
     * @param query firebase query
     * @param marshaller function to convert data
     * @param <T> Type of the return
     * @return return type converted in marshaller
     */
    public <T> Flowable<T> list(Query query, Function<DataSnapshot, T> marshaller, boolean enableRealTimeListener) {
        return Flowable.create(new ListValueOnSubscribe<>(query, marshaller, enableRealTimeListener), BackpressureStrategy.BUFFER);
    }

    /**
     * Run the query and not cancel the listener
     *
     * @param query firebase query
     * @param marshaller function to convert data
     * @param <T> Type of the return
     * @return return type converted in marshaller
     */
    public <T> Flowable<T> listChanges(Query query, Function<DataSnapshot, T> marshaller) {
        return Flowable.create(new ListValueChangesOnSubscribe<>(query, marshaller), BackpressureStrategy.BUFFER);
    }

    /**
     * Create value in database
     *
     * @param value data create in database
     * @param databaseReference where the data will be saved
     * @param <T> Type of the value
     * @return success or error
     */
    public <T> Completable set(T value, DatabaseReference databaseReference) {
        return Completable.create(new SetValueOnSubscriber<T>(value, databaseReference,useListener));
    }

    /**
     * update value in database
     *
     * @param value data create in database
     * @param databaseReference where the data will be saved
     * @param <T> Type of the value
     * @return success or error
     */
    public <T> Completable update(Map<String, T> value, DatabaseReference databaseReference) {
        return Completable.create(new UpdateValueOnSubscriber<T>(value, databaseReference,useListener));
    }

    /**
     *
     * remove end point in database
     *
     * @param databaseReference reference that be removed
     * @return success or error
     */
    public Completable remove(DatabaseReference databaseReference) {
        return Completable.create(new RemoveValueOnSubscriber(databaseReference));
    }

}
