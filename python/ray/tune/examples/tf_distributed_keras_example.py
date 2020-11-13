"""
Adapted from
https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras
"""
import argparse
import os
import tensorflow as tf
import numpy as np
import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.integration.keras import TuneReportCallback
from ray.tune.integration.tensorflow import (DistributedTrainableCreator,
                                             get_num_workers)


def mnist_dataset(batch_size):
    (x_train, y_train), _ = tf.keras.datasets.mnist.load_data()
    # The `x` arrays are in uint8 and have values in the range [0, 255].
    # You need to convert them to float32 with values in the range [0, 1]
    x_train = x_train / np.float32(255)
    y_train = y_train.astype(np.int64)
    train_dataset = tf.data.Dataset.from_tensor_slices(
        (x_train, y_train)).shuffle(60000).repeat().batch(batch_size)
    return train_dataset


def build_and_compile_cnn_model(config):
    model = tf.keras.Sequential([
        tf.keras.Input(shape=(28, 28)),
        tf.keras.layers.Reshape(target_shape=(28, 28, 1)),
        tf.keras.layers.Conv2D(32, 3, activation="relu"),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(config.get("hidden", 128), activation="relu"),
        tf.keras.layers.Dense(10)
    ])
    model.compile(
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        optimizer=tf.keras.optimizers.SGD(
            learning_rate=config.get("lr", 0.05),
            momentum=config.get("momentum", 0.5)),
        metrics=["accuracy"])
    return model


def train_mnist(config, checkpoint_dir=None):
    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
    per_worker_batch_size = 64
    num_workers = get_num_workers()
    global_batch_size = per_worker_batch_size * num_workers
    multi_worker_dataset = mnist_dataset(global_batch_size)
    with strategy.scope():
        multi_worker_model = build_and_compile_cnn_model(config)
    # Define the checkpoint directory to store the checkpoints

    checkpoint_dir = "./training_checkpoints"
    # Name of the checkpoint files
    checkpoint_prefix = os.path.join(checkpoint_dir, "ckpt_{epoch}")
    multi_worker_model.fit(
        multi_worker_dataset,
        epochs=2,
        steps_per_epoch=70,
        callbacks=[
            tf.keras.callbacks.ModelCheckpoint(
                filepath=checkpoint_prefix, save_weights_only=True),
            TuneReportCallback({
                "mean_accuracy": "accuracy"
            })
        ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--num-workers-per-host",
        "-w",
        type=int,
        default=0,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--num-cpus-per-worker",
        "-c",
        type=int,
        default=2,
        help="number of CPUs for this worker")
    parser.add_argument(
        "--num-gpus-per-worker",
        "-g",
        type=int,
        default=0,
        help="number of GPUs for this worker")
    parser.add_argument(
        "--cluster",
        action="store_true",
        default=False,
        help="enables multi-node tuning")
    args = parser.parse_args()
    if args.cluster:
        options = dict(address="auto")
    else:
        options = dict(num_cpus=4)
    ray.init(**options)
    tf_trainable = DistributedTrainableCreator(
        train_mnist,
        num_workers=args.num_workers,
        num_workers_per_host=args.num_workers_per_host,
        num_cpus_per_worker=args.num_cpus_per_worker,
        num_gpus_per_worker=args.num_gpus_per_worker,
    )
    sched = AsyncHyperBandScheduler(
        time_attr="training_iteration",
        metric="mean_accuracy",
        mode="max",
        max_t=400,
        grace_period=20)
    tune.run(
        tf_trainable,
        name="exp",
        scheduler=sched,
        stop={
            "mean_accuracy": 0.99,
            "training_iteration": 10
        },
        num_samples=1,
        config={
            "lr": tune.sample_from(lambda spec: np.random.uniform(0.001, 0.1)),
            "momentum": tune.sample_from(
                lambda spec: np.random.uniform(0.1, 0.9)),
            "hidden": tune.sample_from(
                lambda spec: np.random.randint(32, 512)),
        })
