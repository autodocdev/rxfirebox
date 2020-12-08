package br.com.autodoc.rxfirebox.firestore;


import androidx.annotation.NonNull;

import com.google.android.gms.tasks.OnFailureListener;
import com.google.android.gms.tasks.OnSuccessListener;
import com.google.firebase.firestore.DocumentReference;

import br.com.autodoc.rxfirebox.Executor;
import io.reactivex.CompletableEmitter;
import io.reactivex.CompletableOnSubscribe;

public class RemoveValueOnSubscriber<T> implements CompletableOnSubscribe {

    private final DocumentReference reference;

    public RemoveValueOnSubscriber(DocumentReference reference) {
        this.reference = reference;
    }

    @Override
    public void subscribe(CompletableEmitter e) throws Exception {
        reference.delete()
                .addOnSuccessListener(Executor.Companion.executeThreadPoolExecutor(), new RxCompletionListener(e))
                .addOnFailureListener(new RxCompletionListener(e));

    }

    private static class RxCompletionListener<T> implements OnSuccessListener<Void>,OnFailureListener {

        private final CompletableEmitter subscriber;

        public RxCompletionListener(CompletableEmitter subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onSuccess(@NonNull Void aVoid) {
            subscriber.onComplete();
        }

        @Override
        public void onFailure(@NonNull Exception e) {
            subscriber.onError(e);
        }
    }
}
