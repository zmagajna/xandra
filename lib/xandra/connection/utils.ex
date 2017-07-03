import Kernel, except: [send: 2]

defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{
    Connection,
    ConnectionError,
    Frame,
    Protocol
  }

  @spec recv_frame(Connection.handler, nil | module) ::
        {:ok, Frame.t} | {:error, :closed | :inet.posix}
  def recv_frame(handler, compressor \\ nil) when is_atom(compressor) do
    length = Frame.header_length()

    with {:ok, header} <- recv(handler, length) do
      case Frame.body_length(header) do
        0 ->
          {:ok, Frame.decode(header)}
        body_length ->
          with {:ok, body} <- recv(handler, body_length),
               do: {:ok, Frame.decode(header, body, compressor)}
      end
    end
  end

  @spec request_options(Connection.handler) :: {:ok, term} | {:error, ConnectionError.t}
  def request_options(handler) do
    payload =
      Frame.new(:options)
      |> Protocol.encode_request(nil)
      |> Frame.encode()

    with :ok <- send(handler, payload),
         {:ok, %Frame{} = frame} <- recv_frame(handler) do
      {:ok, Protocol.decode_response(frame)}
    else
      {:error, reason} ->
        {:error, ConnectionError.new("request options", reason)}
    end
  end

  @spec startup_connection(Connection.handler, map, nil | module) :: :ok | {:error, ConnectionError.t}
  def startup_connection(handler, requested_options, compressor \\ nil, options \\ [])
      when is_map(requested_options) and is_atom(compressor) do
    # We have to encode the STARTUP frame without compression as in this frame
    # we tell the server which compression algorithm we want to use.
    payload =
      Frame.new(:startup)
      |> Protocol.encode_request(requested_options)
      |> Frame.encode()

    # However, we need to pass the compressor module around when we
    # receive the response to this frame because if we said we want to use
    # compression, this response is already compressed.
    with :ok <- send(handler, payload),
         {:ok, frame} <- recv_frame(handler, compressor) do
      case frame do
        %Frame{body: <<>>} ->
          :ok
        %Frame{kind: :authenticate} ->
          authenticate_connection(handler, requested_options, compressor, options)
        _ ->
          raise "protocol violation, got unexpected frame: #{inspect(frame)}"
      end
    else
      {:error, reason} ->
        {:error, ConnectionError.new("startup connection", reason)}
    end
  end

  defp authenticate_connection(handler, requested_options, compressor, options) do
    payload =
      Frame.new(:auth_response)
      |> Protocol.encode_request(requested_options, options)
      |> Frame.encode()

    with :ok <- send(handler, payload),
         {:ok, frame} <- recv_frame(handler, compressor) do
      case frame do
        %Frame{kind: :auth_success} -> :ok
        %Frame{kind: :error} -> {:error, Protocol.decode_response(frame)}
        _ -> raise "protocol violation, got unexpected frame: #{inspect(frame)}"
      end
    else
      {:error, reason} ->
        {:error, ConnectionError.new("authenticate connection", reason)}
    end
  end

  def connect(address, port, options, timeout) do
    with {:ok, socket} <- :gen_tcp.connect(address, port, options, timeout) do
      {:ok, {:gen_tcp, socket}}
    end
  end

  def upgrade_protocol(handler, false), do: {:ok, handler}
  def upgrade_protocol(handler, true), do: upgrade_protocol(handler, [])
  def upgrade_protocol({:gen_tcp, socket}, options) when is_list(options) do
    with {:ok, socket} <- :ssl.connect(socket, options) do
      {:ok, {:ssl, socket}}
    end
  end

  def close({module, socket}) do
    module.close(socket)
  end

  def send({module, socket}, payload) do
    module.send(socket, payload)
  end

  def recv({module, socket}, length) do
    module.recv(socket, length)
  end
end
