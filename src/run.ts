import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    maxThreads = Math.max(0, maxThreads);

    executor.start();

    try {
        const taskChains = new Map<number, Promise<void>>();
        const activeTasks = new Set<Promise<void>>();

        for await (const task of queue) {
            const { targetId } = task;

            if (maxThreads > 0 && activeTasks.size >= maxThreads) {
                await Promise.race(activeTasks);
            }

            const previousTask = taskChains.get(targetId) || Promise.resolve();
            const currentTask = previousTask.then(() => executor.executeTask(task));

            taskChains.set(targetId, currentTask);

            const wrappedTask = currentTask.finally(() => activeTasks.delete(wrappedTask));

            activeTasks.add(wrappedTask);
        }

        await Promise.all(activeTasks);
    } finally {
        executor.stop();
    }
}