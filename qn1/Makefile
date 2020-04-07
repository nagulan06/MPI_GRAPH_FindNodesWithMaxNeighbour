all:
	mpic++ plot.cpp -o plot_data
	mpic++ max.cpp -o max
clean:
	rm plot_data max output.txt
run_plot:
	srun --pty --export=ALL --nodes=1 --tasks-per-node=2 mpirun --mca btl_base_warn_component_unused 0 plot_data
run:
	srun --pty --export=ALL --nodes=1 --tasks-per-node=2 mpirun --mca btl_base_warn_component_unused 0 max

