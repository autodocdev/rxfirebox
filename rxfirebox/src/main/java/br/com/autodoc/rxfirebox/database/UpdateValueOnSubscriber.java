package br.com.autodoc.rxfirebox.database;

import androidx.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.database.DatabaseReference;

import java.util.Map;

import io.reactivex.CompletableEmitter;
import io.reactivex.CompletableOnSubscribe;

public class UpdateValueOnSubscriber<T> implements CompletableOnSubscribe {

    private final Map<String, T> value;
    private final DatabaseReference databaseReference;
    private final boolean useListener;

    public UpdateValueOnSubscriber(Map<String, T> value, DatabaseReference databaseReference, boolean useListener) {
        this.value = value;
        this.databaseReference = databaseReference;
        this.useListener = useListener;
    }

    @Override
    public void subscribe(CompletableEmitter e) throws Exception {
        if (useListener) {
            databaseReference.setValue(value).addOnCompleteListener(new RxCompletionListener(e));
        } else {
            databaseReference.updateChildren((Map<String, Object>) value);
            e.onComplete();
        }
    }

    private static class RxCompletionListener implements OnCompleteListener<Void> {

        private final CompletableEmitter subscriber;

        public RxCompletionListener(CompletableEmitter subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onComplete(@NonNull Task<Void> task) {
            if (task.getException() != null) {
                subscriber.onError(task.getException());
            } else {
                subscriber.onComplete();
            }
        }
    }

}
