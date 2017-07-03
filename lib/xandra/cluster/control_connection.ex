defmodule Xandra.Cluster.ControlConnection do
  use Connection

  alias Xandra.{Frame, Protocol, Connection.Utils}

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [
    :address,
    :port,
    :cluster,
    :handler,
    :options,
    new: true,
    buffer: <<>>,
  ]

  def start_link(cluster, address, port, options) do
    state = %__MODULE__{cluster: cluster, address: address, port: port, options: options}
    Connection.start_link(__MODULE__, state)
  end

  def init(state) do
    {:connect, :init, state}
  end

  def connect(_action, %__MODULE__{address: address, port: port, options: options} = state) do
    case Utils.connect(address, port, @socket_options, @default_timeout) do
      {:ok, handler} ->
        state = %{state | handler: handler}
        with {:ok, handler} <- upgrade_protocol(handler, options),
             {:ok, supported_options} <- Utils.request_options(handler),
             :ok <- startup_connection(handler, supported_options, options),
             :ok <- register_to_events(handler),
             :ok <- activate(handler),
             {:ok, state} <- report_active(state) do
          {:ok, state}
        else
          {:error, _reason} = error ->
            {:connect, :reconnect, state} = disconnect(error, state)
            {:backoff, @default_backoff, state}
        end
      {:error, _reason} ->
        {:backoff, @default_backoff, state}
    end
  end

  def handle_info(message, %__MODULE__{handler: {:gen_tcp, socket}} = state) do
    case message do
      {:tcp_error, ^socket, reason} ->
        {:disconnect, {:error, reason}, state}
      {:tcp_closed, ^socket} ->
        {:disconnect, {:error, :closed}, state}
      {:tcp, ^socket, data} ->
        state = %{state | buffer: state.buffer <> data}
        {:noreply, report_event(state)}
    end
  end

  def handle_info(message, %__MODULE__{handler: {:ssl, socket}} = state) do
    case message do
      {:ssl_error, ^socket, reason} ->
        {:disconnect, {:error, reason}, state}
      {:ssl_closed, ^socket} ->
        {:disconnect, {:error, :closed}, state}
      {:ssl, ^socket, data} ->
        state = %{state | buffer: state.buffer <> data}
        {:noreply, report_event(state)}
    end
  end

  def disconnect({:error, _reason}, %__MODULE__{} = state) do
    Utils.close(state.handler)
    {:connect, :reconnect, %{state | handler: nil, buffer: <<>>}}
  end

  defp report_active(%{new: false} = state) do
    {:ok, state}
  end

  defp report_active(%{new: true, cluster: cluster, handler: handler} = state) do
    with {:ok, {address, port}} <- peername(handler) do
      Xandra.Cluster.activate(cluster, address, port)
      {:ok, %{state | new: false, address: address}}
    end
  end

  defp upgrade_protocol(handler, options) do
    Utils.upgrade_protocol(handler, Keyword.fetch!(options, :encryption))
  end

  defp startup_connection(handler, supported_options, options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(handler, requested_options, nil, options)
  end

  defp register_to_events(handler) do
    payload =
      Frame.new(:register)
      |> Protocol.encode_request(["STATUS_CHANGE"])
      |> Frame.encode()

    with :ok <- Utils.send(handler, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(handler) do
      :ok = Protocol.decode_response(frame)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp report_event(%{cluster: cluster, buffer: buffer} = state) do
    case decode_frame(buffer) do
      {frame, rest} ->
        status_change = Protocol.decode_response(frame)
        Logger.debug("Received STATUS_CHANGE event: #{inspect(status_change)}")
        Xandra.Cluster.update(cluster, status_change)
        report_event(%{state | buffer: rest})
      :error ->
        state
    end
  end

  defp decode_frame(buffer) do
    header_length = Frame.header_length()
    case buffer do
      <<header::size(header_length)-bytes, rest::binary>> ->
        body_length = Frame.body_length(header)
        case rest do
          <<body::size(body_length)-bytes, rest::binary>> ->
            {Frame.decode(header, body), rest}
          _ ->
            :error
        end
      _ ->
        :error
    end
  end

  defp activate({module, socket}) do
    module.setopts(socket, active: true)
  end

  defp peername({module, socket}) do
    module.peername(socket)
  end
end
